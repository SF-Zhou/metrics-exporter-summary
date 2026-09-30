use super::{invalid, wire, ProtocolError, ProtocolLimits, MAX_ACK_BYTES};
use metrics_summary_core::{Row, ValidationLimits};

// The wire schema is nonrecursive: every container below has a known element
// type and nesting depth. Inspect it without allocating before Serde sees any
// untrusted array/string length. Maps, extensions and unknown fields are rejected.
struct Scan<'a> {
    bytes: &'a [u8],
    estimated: usize,
    budget: usize,
}
impl<'a> Scan<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], ProtocolError> {
        if count > self.bytes.len() {
            return Err(invalid("truncated MessagePack value"));
        }
        let (head, tail) = self.bytes.split_at(count);
        self.bytes = tail;
        Ok(head)
    }
    fn marker(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn uint_bytes(&mut self, count: usize) -> Result<u64, ProtocolError> {
        let mut bytes = [0; 8];
        bytes[8 - count..].copy_from_slice(self.take(count)?);
        Ok(u64::from_be_bytes(bytes))
    }
    fn length(&mut self, count: usize) -> Result<usize, ProtocolError> {
        usize::try_from(self.uint_bytes(count)?).map_err(|_| invalid("length overflow"))
    }
    fn array(&mut self, max: usize) -> Result<usize, ProtocolError> {
        let length = match self.marker()? {
            marker @ 0x90..=0x9f => usize::from(marker & 0x0f),
            0xdc => self.length(2)?,
            0xdd => self.length(4)?,
            _ => return Err(invalid("expected MessagePack array")),
        };
        if length > max {
            return Err(invalid("array count exceeds limit"));
        }
        // Every array member takes at least one byte. This also rejects huge
        // announced counts backed by tiny or truncated bodies before allocation.
        if length > self.bytes.len() {
            return Err(invalid("truncated MessagePack array"));
        }
        Ok(length)
    }
    fn tuple(&mut self, count: usize) -> Result<(), ProtocolError> {
        if self.array(count)? != count {
            return Err(invalid("wrong MessagePack tuple length"));
        }
        Ok(())
    }
    fn unsigned(&mut self) -> Result<u64, ProtocolError> {
        match self.marker()? {
            marker @ 0x00..=0x7f => Ok(u64::from(marker)),
            0xcc => self.uint_bytes(1),
            0xcd => self.uint_bytes(2),
            0xce => self.uint_bytes(4),
            0xcf => self.uint_bytes(8),
            _ => Err(invalid("expected unsigned integer")),
        }
    }
    fn signed(&mut self) -> Result<i64, ProtocolError> {
        match self.bytes.first().copied() {
            Some(0x00..=0x7f | 0xcc..=0xcf) => {
                i64::try_from(self.unsigned()?).map_err(|_| invalid("signed integer overflow"))
            }
            Some(marker @ 0xe0..=0xff) => {
                self.take(1)?;
                Ok(i64::from(marker as i8))
            }
            Some(0xd0..=0xd3) => {
                let marker = self.marker()?;
                let count = 1 << (marker - 0xd0);
                let bits = self.uint_bytes(count)?;
                let shift = 64 - count * 8;
                Ok(((bits << shift) as i64) >> shift)
            }
            _ => Err(invalid("expected signed integer")),
        }
    }
    fn u32(&mut self) -> Result<(), ProtocolError> {
        u32::try_from(self.unsigned()?).map_err(|_| invalid("u32 overflow"))?;
        Ok(())
    }
    fn i32(&mut self) -> Result<(), ProtocolError> {
        i32::try_from(self.signed()?).map_err(|_| invalid("i32 overflow"))?;
        Ok(())
    }
    fn float64(&mut self) -> Result<(), ProtocolError> {
        if self.marker()? != 0xcb {
            return Err(invalid("expected float64 marker"));
        }
        self.take(8)?;
        Ok(())
    }
    fn nil(&mut self) -> bool {
        if self.bytes.first() == Some(&0xc0) {
            self.bytes = &self.bytes[1..];
            true
        } else {
            false
        }
    }
    fn string(&mut self, max: usize) -> Result<(), ProtocolError> {
        let length = match self.marker()? {
            marker @ 0xa0..=0xbf => usize::from(marker & 0x1f),
            0xd9 => self.length(1)?,
            0xda => self.length(2)?,
            0xdb => self.length(4)?,
            _ => return Err(invalid("expected MessagePack string")),
        };
        if length > max {
            return Err(invalid("string length exceeds limit"));
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid("invalid UTF-8"))?;
        Ok(())
    }
    fn id(&mut self) -> Result<(), ProtocolError> {
        self.tuple(2)?;
        let length = match self.marker()? {
            0xc4 => self.length(1)?,
            0xc5 => self.length(2)?,
            0xc6 => self.length(4)?,
            _ => return Err(invalid("expected binary UUID")),
        };
        if length != 16 {
            return Err(invalid("invalid UUID length"));
        }
        self.take(16)?;
        self.unsigned()?;
        Ok(())
    }
    fn charge(&mut self, count: usize, bytes: usize) -> Result<(), ProtocolError> {
        self.estimated = self.estimated.saturating_add(count.saturating_mul(bytes));
        if self.estimated > self.budget {
            return Err(invalid("decoding workspace exceeds limit"));
        }
        Ok(())
    }
    fn entries(&mut self, max: usize, key: usize, value: usize) -> Result<(), ProtocolError> {
        let count = self.array(max)?;
        self.charge(count, 160)?;
        for _ in 0..count {
            self.tuple(2)?;
            self.string(key)?;
            self.string(value)?;
        }
        Ok(())
    }
    fn source(&mut self, limits: &ValidationLimits) -> Result<(), ProtocolError> {
        self.tuple(4)?;
        for _ in 0..3 {
            self.string(limits.max_source_field_bytes)?;
        }
        self.entries(
            limits.max_source_attributes,
            limits.max_attribute_key_bytes,
            limits.max_attribute_value_bytes,
        )
    }
    fn row(&mut self, limits: &ValidationLimits) -> Result<(), ProtocolError> {
        self.tuple(5)?;
        self.unsigned()?;
        self.string(limits.max_name_bytes)?;
        self.entries(
            limits.max_labels,
            limits.max_label_key_bytes,
            limits.max_label_value_bytes,
        )?;
        if !self.nil() {
            self.string(limits.max_unit_bytes)?;
        }
        self.tuple(2)?;
        match self.unsigned()? {
            0 => {
                self.tuple(8)?;
                self.unsigned()?;
                for _ in 0..7 {
                    self.float64()?;
                }
            }
            1 => {
                self.unsigned()?;
            }
            2 => {
                self.signed()?;
            }
            _ => return Err(invalid("unknown instrument kind")),
        }
        Ok(())
    }
    fn finish(self) -> Result<(), ProtocolError> {
        if !self.bytes.is_empty() {
            return Err(invalid("trailing MessagePack data"));
        }
        Ok(())
    }
}

pub(super) fn preflight(bytes: &[u8], limits: &ProtocolLimits) -> Result<(), ProtocolError> {
    if bytes.len() > limits.max_encoded_bytes {
        return Err(invalid("encoded batch exceeds limit"));
    }
    let mut scan = Scan {
        bytes,
        estimated: bytes.len(),
        budget: limits
            .max_encoded_bytes
            .saturating_add(limits.validation.max_batch_bytes.saturating_mul(2)),
    };
    scan.tuple(8)?;
    scan.u32()?;
    scan.u32()?;
    scan.i32()?;
    scan.id()?;
    scan.source(&limits.validation)?;
    scan.signed()?;
    scan.unsigned()?;
    let count = scan.array(limits.validation.max_rows)?;
    scan.charge(
        count,
        std::mem::size_of::<wire::Row>() + std::mem::size_of::<Row>(),
    )?;
    for _ in 0..count {
        scan.row(&limits.validation)?;
    }
    scan.finish()
}

pub(super) fn preflight_ack(bytes: &[u8]) -> Result<(), ProtocolError> {
    if bytes.len() > MAX_ACK_BYTES {
        return Err(invalid("ACK too large"));
    }
    let mut scan = Scan {
        bytes,
        estimated: bytes.len(),
        budget: MAX_ACK_BYTES,
    };
    scan.tuple(5)?;
    scan.u32()?;
    if !scan.nil() {
        scan.id()?;
    }
    scan.i32()?;
    scan.i32()?;
    scan.string(1024)?;
    scan.finish()
}

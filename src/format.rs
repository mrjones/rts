pub const VAL_WIDTH: usize = 8;
pub const REC_WIDTH: usize = 2 * VAL_WIDTH;

#[derive(Debug,PartialEq)]
pub struct Rec {
    pub timestamp: u64,
    pub value: u64,
}

pub fn store_rec(rec: &Rec, buf: &mut [u8]) {
    assert_eq!(2 * VAL_WIDTH, buf.len());

    store(rec.timestamp, &mut buf[0..VAL_WIDTH]);
    store(rec.value, &mut buf[VAL_WIDTH..(2*VAL_WIDTH)]);
}

pub fn load_rec(buf: &[u8]) -> Rec {
    assert_eq!(2 * VAL_WIDTH, buf.len());
    
    let value = load(&buf[VAL_WIDTH..(2*VAL_WIDTH)]);
    let timestamp = load(&buf[0..VAL_WIDTH]);

    return Rec{timestamp: timestamp, value: value};
}

pub fn load(buf: &[u8]) -> u64 {
    assert_eq!(VAL_WIDTH, buf.len());

    let mut acc : u64 = 0;
    for b in 0..VAL_WIDTH {
        acc += (buf[b] as u64) << (8 * b);
    }
    return acc;
}

pub fn store(n: u64, buf: &mut [u8]) {
    assert_eq!(VAL_WIDTH, buf.len());

    for b in 0..VAL_WIDTH {
        buf[b] = ((n >> (8 * b)) % 256) as u8;
    }
}

#[cfg(test)]
mod test {
    fn round_trip(n: u64) -> u64{
        let mut buf : [u8; super::VAL_WIDTH] = [0; super::VAL_WIDTH];

        super::store(n, &mut buf);
        return super::load(&buf);
    }

    #[test]
    fn encode_decode() {
        assert_eq!(0, round_trip(0));
        assert_eq!(1, round_trip(1));
        assert_eq!(255, round_trip(255));
        assert_eq!(256, round_trip(256));
        assert_eq!(1234567890, round_trip(1234567890));
        assert_eq!(u64::max_value(), round_trip(u64::max_value()));

    }

    fn round_trip_rec(rec: super::Rec) {
        let mut buf : [u8; super::REC_WIDTH] = [0; super::REC_WIDTH];
        super::store_rec(&rec, &mut buf);
        let rec2 = super::load_rec(&buf);
        assert_eq!(rec, rec2);
    }

    #[test]
    fn encode_decode_rec() {
        round_trip_rec(super::Rec{
            timestamp: 1234567890,
            value: 257,
        });
    }
}

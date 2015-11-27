use std::io;

pub struct TableBuilder;

impl TableBuilder {
    pub fn write<'a, I: Iterator<Item=(&'a u64, &'a u64)>>(data: I) -> io::Result<()> {
        for (k, v) in data {
            println!("{}={}", k, v);
        }
        
        return Ok(());
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    #[test]
    fn write_table() {
        let mut map = BTreeMap::new();
        map.insert(1, 2);
        map.insert(3, 4);

        super::TableBuilder::write(map.iter()).unwrap();
    }
}

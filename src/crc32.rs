pub struct Crc32 {
    table: [u32; 256],
    value: u32
}

impl Crc32 {
    pub fn tocrc(buf: &[u8]) -> u32 {
        let mut crc = Crc32 { table: [0; 256], value: 0xffffffff };
        for i in 0..256 {
            crc.table[i] = Crc32::calc_table(i as u32);
        }
        for &i in buf {
            crc.value = crc.calc_value(i as u32);
        }
        crc.value ^ 0xffffffffu32
    }

    fn calc_value(&self, b: u32) -> u32 {
        let index = ((self.value ^ b) & 0xFF) as usize;
        self.table[index] ^ (self.value >> 8)
    }

    fn calc_table(index: u32) -> u32{
        let mut temp = index;
        for _ in 0..8 {
            if temp & 1 == 0 {
                temp = temp >> 1
            } else {
                temp = 0xedb88320u32 ^ (temp >> 1)
            }
        }
        temp
    }
}

pub struct Crc32 {
    table: [u32; 256],
    value: u32
}

impl Crc32 {
    pub fn tocrc(buf: &[u8]) -> u32 {
        let mut crc = Crc32 { table: [0; 256], value: 0xffffffff };
        (0..256).map(|x| crc.table[x as usize] = Crc32::calc_table(x as u32));
        buf.iter().map(|&x| crc.value = crc.calc_value(x as u32));
        crc.value ^ 0xffffffff
    }

    fn calc_value(&self, b: u32) -> u32 {
        let index = ((self.value ^ b) & 0xFF) as usize;
        self.table[index] ^ (self.value >> 8)
    }

    fn calc_table(index: u32) -> u32{
        let mut temp = index as u32;
        for _ in 0..8 {
            if temp & 1 == 0 {
                temp = temp >> 1
            } else {
                temp = 0xedb88320 ^ (temp >> 1)
            }
        }
        temp
    }
}

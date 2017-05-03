use std::mem;
use std::cmp;
use std::io;
use std::io::prelude::*;
use std::hash::Hasher;

use lz4_compress;
use twox_hash::XxHash32;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use error::{Error, Result};

pub fn compress(src: &[u8]) -> Vec<u8> {
    lz4_compress::compress(src)
}

pub fn uncompress(src: &[u8]) -> Result<Vec<u8>> {
    Ok(try!(lz4_compress::decompress(src)))
}

fn xxhash32(src: &[u8], seed: u32) -> u32 {
    let mut hasher = XxHash32::with_seed(seed);
    hasher.write(src);
    hasher.finish() as u32
}

pub const LZ4_MAGIC: u32 = 0x184D2204;
pub const LZ4_FRAME_INCOMPRESSIBLE_MASK: u32 = 0x80000000;
pub const FLAG_VERSION_SHIFT: u8 = 6;
pub const FLAG_INDEPENDENT_SHIFT: u8 = 5;
pub const FLAG_BLOCK_CHECKSUM_SHIFT: u8 = 4;
pub const FLAG_BLOCK_SIZE_SHIFT: u8 = 3;
pub const FLAG_CONTENT_CHECKSUM_SHIFT: u8 = 2;
pub const BLOCKSIZE_SHIFT: u8 = 4;
pub const BLOCKSIZE_MASK: u8 = 0x7;
pub const BLOCKSIZE_64KB: u8 = 4;
pub const BLOCKSIZE_256KB: u8 = 5;
pub const BLOCKSIZE_1MB: u8 = 6;
pub const BLOCKSIZE_4MB: u8 = 7;

/// see also: LZ4 Frame Format Description <https://github.com/lz4/lz4/wiki/lz4_Frame_format.md>
pub struct Lz4Frame {
    flag: u8,
    bd: u8,
    size: Option<u64>,
    content_hash: Option<XxHash32>,
}

impl Lz4Frame {
    pub fn version(&self) -> u8 {
        (self.flag >> FLAG_VERSION_SHIFT) & 0x3
    }

    pub fn independent(&self) -> bool {
        ((self.flag >> FLAG_INDEPENDENT_SHIFT) & 0x1) != 0
    }

    pub fn block_checksum(&self) -> bool {
        ((self.flag >> FLAG_BLOCK_CHECKSUM_SHIFT) & 0x1) != 0
    }

    pub fn block_size(&self) -> bool {
        ((self.flag >> FLAG_BLOCK_SIZE_SHIFT) & 0x1) != 0
    }

    pub fn content_checksum(&self) -> bool {
        ((self.flag >> FLAG_CONTENT_CHECKSUM_SHIFT) & 0x1) != 0
    }

    pub fn block_max_size(&self) -> Option<usize> {
        match (self.bd >> BLOCKSIZE_SHIFT) & BLOCKSIZE_MASK {
            BLOCKSIZE_64KB => Some(64 * 1024),
            BLOCKSIZE_256KB => Some(256 * 1024),
            BLOCKSIZE_1MB => Some(1024 * 1024),
            BLOCKSIZE_4MB => Some(4 * 1024 * 1024),
            _ => None,
        }
    }

    pub fn header_checksum(&self, include_magic: bool) -> u8 {
        let mut header = Vec::with_capacity(14);

        if include_magic {
            header.write_u32::<LittleEndian>(LZ4_MAGIC).unwrap();
        }

        header.write(&[self.flag, self.bd]).unwrap();

        if let Some(size) = self.size {
            header.write_u64::<LittleEndian>(size).unwrap();
        }

        ((xxhash32(&header[..], 0) >> 8) & 0xFF) as u8
    }

    pub fn read_header<R: Read>(reader: &mut R,
                                verify_header_checksum: bool,
                                header_checksum_include_magic: bool)
                                -> Result<Lz4Frame> {
        if try!(reader.read_u32::<LittleEndian>()) != LZ4_MAGIC {
            return Err(Error::InvalidInputLz4("magic error".to_owned()));
        }

        let flag = try!(reader.read_u8());
        let bd = try!(reader.read_u8());
        let size = if ((flag >> FLAG_BLOCK_SIZE_SHIFT) & 0x1) == 0x1 {
            Some(try!(reader.read_u64::<LittleEndian>()))
        } else {
            None
        };
        let checksum = try!(reader.read_u8());

        let frame = Lz4Frame {
            flag: flag,
            bd: bd,
            size: size,
            content_hash: if (flag >> FLAG_CONTENT_CHECKSUM_SHIFT) == 0 {
                None
            } else {
                Some(XxHash32::with_seed(0))
            },
        };

        if verify_header_checksum &&
           frame.header_checksum(header_checksum_include_magic) != checksum {
            Err(Error::InvalidInputLz4("header checksum error".to_owned()))
        } else {
            Ok(frame)
        }
    }

    pub fn write_header<W: Write>(&self,
                                  writer: &mut W,
                                  header_checksum_include_magic: bool)
                                  -> io::Result<usize> {
        try!(writer.write_u32::<LittleEndian>(LZ4_MAGIC));
        try!(writer.write_u8(self.flag));
        try!(writer.write_u8(self.bd));

        let mut wrote = mem::size_of_val(&LZ4_MAGIC) + 2;

        if let Some(size) = self.size {
            try!(writer.write_u64::<LittleEndian>(size));
            wrote += mem::size_of_val(&size);
        }

        try!(writer.write_u8(self.header_checksum(header_checksum_include_magic)));
        wrote += 1;

        Ok(wrote)
    }

    pub fn read_block<R: Read>(&self, reader: &mut R, buf: &mut Vec<u8>) -> io::Result<usize> {
        let (block_size, mut compressed_data) = match try!(reader.read_u32::<LittleEndian>()) {
            0 => (0, None),
            n if n & LZ4_FRAME_INCOMPRESSIBLE_MASK == 0 => (n as usize, Some(vec![0; n as usize])),
            n @ _ => ((n ^ LZ4_FRAME_INCOMPRESSIBLE_MASK) as usize, None),
        };

        if block_size == 0 {
            if self.content_checksum() {
                try!(reader.read_u32::<LittleEndian>()); // TODO: verify this content checksum
            }
        } else {
            let read_buf = if let Some(data) = compressed_data.as_mut() {
                &mut data[..]
            } else {
                let off = buf.len();
                buf.resize(off + block_size, 0);
                &mut buf[off..]
            };

            let read = try!(reader.read(read_buf));

            trace!("read {} bytes: {:?}", read, &read_buf[..cmp::min(16, read)]);

            if self.block_checksum() &&
               try!(reader.read_u32::<LittleEndian>()) != xxhash32(read_buf, 0) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "block checksum error"));
            }
        }

        if let Some(data) = compressed_data.as_ref() {
            let mut uncompressed_data = try!(uncompress(&data[..block_size]).map_err(|_| {
                                                  io::Error::new(io::ErrorKind::InvalidData,
                                                                 "lz4 decode error")
                                              }));
            let uncompressed_size = uncompressed_data.len();

            debug!("uncompress {} bytes block: {:?}",
                   uncompressed_size,
                   &uncompressed_data[..cmp::min(16, uncompressed_data.len())]);

            buf.append(&mut uncompressed_data);

            Ok(uncompressed_size)
        } else {
            Ok(block_size)
        }
    }

    pub fn write_block<W: Write>(&mut self, writer: &mut W, buf: &[u8]) -> io::Result<usize> {
        debug!("write {} bytes block: {:?}", buf.len(), &buf[..cmp::min(16, buf.len())]);

        let compressed_data = compress(buf);
        try!(writer.write_u32::<LittleEndian>(compressed_data.len() as u32));
        let mut wrote = mem::size_of::<u32>();
        wrote += try!(writer.write(&compressed_data[..]));
        if self.block_checksum() {
            try!(writer.write_u32::<LittleEndian>(xxhash32(&compressed_data[..], 0)));
            wrote += 2;
        }
        if self.content_checksum() {
            if let Some(ref mut hasher) = self.content_hash {
                hasher.write(buf);
            }
        }
        Ok(wrote)
    }

    pub fn write_end_mark<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        try!(writer.write_u32::<LittleEndian>(0));
        if self.content_checksum() {
            let checksum = if let Some(ref hasher) = self.content_hash {
                hasher.finish() as u32
            } else {
                0
            };

            try!(writer.write_u32::<LittleEndian>(checksum));
        }
        Ok(mem::size_of::<u32>())
    }
}

pub struct Lz4Reader<R> {
    reader: R,
    frame: Lz4Frame,
    block: Vec<u8>,
    finished: bool,
}

impl<R: Read> Lz4Reader<R> {
    pub fn new(mut reader: R,
               verify_header_checksum: bool,
               header_checksum_include_magic: bool)
               -> Result<Self> {
        let frame = try!(Lz4Frame::read_header(&mut reader,
                                               verify_header_checksum,
                                               header_checksum_include_magic));
        let block = Vec::with_capacity(frame.block_max_size().unwrap_or(4096));
        Ok(Lz4Reader {
               reader: reader,
               frame: frame,
               block: block,
               finished: false,
           })
    }
}

impl<R: Read> Read for Lz4Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            Ok(0)
        } else {
            let remaining = if self.block.is_empty() {
                try!(self.frame.read_block(&mut self.reader, &mut self.block))
            } else {
                self.block.len()
            };

            if remaining == 0 {
                self.finished = true;

                Ok(0)
            } else {
                let read = cmp::min(self.block.len(), buf.len());

                buf[..read].copy_from_slice(&self.block[..read]);

                self.block = self.block.split_off(read);

                Ok(read)
            }
        }
    }
}

pub struct Lz4Writer<'a, W: 'a> {
    writer: &'a mut W,
    frame: Lz4Frame,
    buf: Vec<u8>,
}

impl<'a, W: Write> Lz4Writer<'a, W> {
    pub fn new(writer: &'a mut W,
               header_checksum_include_magic: bool,
               block_max_size: u8,
               block_checksum: bool,
               content_checksum: bool)
               -> Result<Lz4Writer<'a, W>> {
        let version = 1;
        let independent = true;
        let flag = (version << FLAG_VERSION_SHIFT) |
                   if independent {
                       1 << FLAG_INDEPENDENT_SHIFT
                   } else {
                       0
                   } |
                   if block_checksum {
                       1 << FLAG_BLOCK_CHECKSUM_SHIFT
                   } else {
                       0
                   } |
                   if content_checksum {
                       1 << FLAG_CONTENT_CHECKSUM_SHIFT
                   } else {
                       0
                   };

        let frame = Lz4Frame {
            flag: flag,
            bd: (block_max_size & BLOCKSIZE_MASK) << BLOCKSIZE_SHIFT,
            size: None,
            content_hash: if content_checksum {
                Some(XxHash32::with_seed(0))
            } else {
                None
            },
        };

        try!(frame.write_header(writer, header_checksum_include_magic));

        let buf = frame
            .block_max_size()
            .map_or_else(|| Vec::new(), |size| Vec::with_capacity(size));

        Ok(Lz4Writer {
               writer: writer,
               frame: frame,
               buf: buf,
           })
    }

    fn write_block(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            debug!("compress {} bytes block: {:?}",
                   self.buf.len(),
                   &self.buf[..cmp::min(16, self.buf.len())]);

            try!(self.frame.write_block(&mut self.writer, &self.buf[..]));

            self.buf.clear();
        }

        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        try!(self.write_block());
        try!(self.frame.write_end_mark(&mut self.writer));

        Ok(())
    }
}

impl<'a, W: Write> Write for Lz4Writer<'a, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = match self.frame.block_max_size() {
            Some(max_size) if self.buf.len() + buf.len() > max_size => {
                let write_size = max_size - self.buf.len();
                self.buf.extend_from_slice(&buf[..write_size]);
                Some(&buf[write_size..])
            }
            _ => {
                self.buf.extend_from_slice(buf);
                None
            }
        };

        try!(self.write_block());

        if let Some(remaining) = remaining {
            try!(self.write(remaining));
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        try!(self.write_block());
        self.writer.flush()
    }
}


#[test]
fn test_compress() {
    use std::iter;

    let comp_msg = compress(b"test");
    assert_eq!(comp_msg, &[64, 116, 101, 115, 116]);

    let comp_msg = compress(iter::repeat(123)
                                .take(1000)
                                .collect::<Vec<u8>>()
                                .as_slice());
    assert_eq!(comp_msg, &[31, 123, 1, 0, 255, 255, 255, 215, 0]);
}

#[test]
fn test_uncompress() {
    use std::iter;

    // The vector should uncompress to "test"
    let msg: Vec<u8> = vec![64, 116, 101, 115, 116];
    let uncomp_msg = String::from_utf8(uncompress(&msg[..]).unwrap()).unwrap();
    assert_eq!(uncomp_msg.as_str(), "test");

    let msg = &[31, 123, 1, 0, 255, 255, 255, 215, 0][..];
    let uncomp_msg = uncompress(msg).unwrap();
    assert_eq!(uncomp_msg,
               iter::repeat(123)
                   .take(1000)
                   .collect::<Vec<u8>>()
                   .as_slice());
}

#[test]
#[should_panic]
fn test_uncompress_panic() {
    let msg: Vec<u8> = vec![192, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116, 0];
    let uncomp_msg = String::from_utf8(uncompress(&msg[..]).unwrap()).unwrap();
    assert_eq!(uncomp_msg.as_str(), "This is test");
}

#[test]
fn test_read_frame() {
    let flag = (1 << FLAG_VERSION_SHIFT) | (1 << FLAG_INDEPENDENT_SHIFT) |
               (1 << FLAG_BLOCK_CHECKSUM_SHIFT);
    let bd = BLOCKSIZE_256KB << BLOCKSIZE_SHIFT;
    let buf = [0x04, 0x22, 0x4D, 0x18,    // magic
               flag,
               bd,
               132,                       // header checksum
               0, 0, 0, 0                 // end mark
               ];
    let mut r = Lz4Reader::new(&buf[..], false, false).unwrap();

    assert_eq!(r.frame.version(), 1);
    assert!(r.frame.independent());
    assert!(r.frame.block_checksum());
    assert!(!r.frame.block_size());
    assert!(!r.frame.content_checksum());
    assert_eq!(r.frame.block_max_size(), Some(256 * 1024));
    assert_eq!(r.frame.header_checksum(false), 132);

    let mut data = vec![];

    assert_eq!(r.read_to_end(&mut data).unwrap(), 0);
}

#[test]
fn test_read_block() {
    let flag = (1 << FLAG_VERSION_SHIFT) | (1 << FLAG_INDEPENDENT_SHIFT) |
               (1 << FLAG_BLOCK_CHECKSUM_SHIFT);
    let bd = BLOCKSIZE_256KB << BLOCKSIZE_SHIFT;
    let buf = [0x04, 0x22, 0x4D, 0x18,      // magic
               flag,
               bd,
               132,                         // header checksum
               5, 0, 0, 0,                  // block size
               64, 116, 101, 115, 116,      // compressed
               137, 205, 203, 160,          // block checksum
               0, 0, 0, 0                   // end mark
               ];
    let mut r = Lz4Reader::new(&buf[..], true, false).unwrap();

    let mut data = Vec::new();

    assert_eq!(r.read_to_end(&mut data).unwrap(), 4);
    assert_eq!(String::from_utf8(data).unwrap(), "test");
}

#[test]
fn test_write_frame() {
    let mut buf = vec![];
    {
        let mut w = Lz4Writer::new(&mut buf, false, BLOCKSIZE_64KB, false, false).unwrap();

        w.close().unwrap();
    }

    let flag = (1 << FLAG_VERSION_SHIFT) | (1 << FLAG_INDEPENDENT_SHIFT);
    let bd = BLOCKSIZE_64KB << BLOCKSIZE_SHIFT;

    assert_eq!(buf.as_slice(),
               &[0x04, 0x22, 0x4D, 0x18,    // magic
                 flag,
                 bd,
                 130,                       // header checksum
                 0, 0, 0, 0                 // end mark
                 ]);
}

#[test]
fn test_write_block() {
    let mut buf = vec![];
    {
        let mut w = Lz4Writer::new(&mut buf, false, BLOCKSIZE_64KB, true, false).unwrap();

        assert_eq!(w.write(b"test").unwrap(), 4);

        w.close().unwrap();
    }

    let flag = (1 << FLAG_VERSION_SHIFT) | (1 << FLAG_INDEPENDENT_SHIFT) |
               (1 << FLAG_BLOCK_CHECKSUM_SHIFT);
    let bd = BLOCKSIZE_64KB << BLOCKSIZE_SHIFT;

    assert_eq!(buf.as_slice(),
               &[0x04, 0x22, 0x4D, 0x18,        // magic
                 flag,
                 bd,
                 173,                           // header checksum
                 5, 0, 0, 0,                    // block size
                 64, 116, 101, 115, 116,        // compressed
                 137, 205, 203, 160,            // block checksum
                 0, 0, 0, 0                     // end mark
                 ]);
}
use std::io::{self};
use zstd;


pub fn uncompress<R>(source: R) -> io::Result<Vec<u8>>  
where
    R: io::Read,
{
    zstd::decode_all(source)
}

pub fn compress<R>(source: R, level: i32) -> io::Result<Vec<u8>>  
where
    R: io::Read,
{
    zstd::encode_all(source, level)
}

#[test]
fn test_uncompress() {
    use std::io::Cursor;
    // The vector should uncompress to "This is a Test Message"

    // Compress it 
    let uncompressed_message = "This is a Test Message".as_bytes();
    let comp_msg = compress(Cursor::new(uncompressed_message), 3).unwrap();

    // Uncompress it
    let uncomp_msg = String::from_utf8(uncompress(Cursor::new(comp_msg)).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "This is a Test Message");
}

#[test]
#[should_panic]
fn test_uncompress_panic() {
    use std::io::Cursor;
    let msg: Vec<u8> = vec![
        12,
        42,
        84,
        104,
        105,
        115,
        32,
        105,
        115,
        32,
        116,
        101,
        115,
        116,
    ];
    let uncomp_msg = String::from_utf8(uncompress(Cursor::new(msg)).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "This is test");
}

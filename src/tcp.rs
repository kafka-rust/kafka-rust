use std::string::String;
use std::vec::Vec;
use std::io::TcpStream;

pub trait KafkaClient
{
    fn new() -> Self;
    fn add_host(&mut self, ip: String, port: String);
    fn remove_host(&mut self, index: usize);
    fn send_message(&mut self, message: String);
}

use bytes::{BytesMut, Buf};
use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(os) => match os {
                Some(s) => {
                    let mut res = format!("${}\r\n", s.len()).into_bytes();
                    res.extend(s);
                    res.extend(b"\r\n");
                    res
                }
                None => b"$-1\r\n".to_vec(),
            },
            RespValue::Array(oa) => match oa {
                Some(a) => {
                    let mut res = format!("*{}\r\n", a.len()).into_bytes();
                    for val in a {
                        res.extend(val.serialize());
                    }
                    res
                }
                None => b"*-1\r\n".to_vec(),
            },
        }
    }

    pub fn to_command_string(&self) -> Option<String> {
        match self {
            RespValue::Array(Some(parts)) => {
                let mut res = String::new();
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 { res.push(' '); }
                    match part {
                        RespValue::BulkString(Some(b)) => {
                            let s = String::from_utf8_lossy(b);
                            if s.contains(' ') || s.contains('\n') || s.contains('\r') || s.is_empty() {
                                let escaped = s.replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
                                res.push_str(&format!("\"{}\"", escaped));
                            } else {
                                res.push_str(&s);
                            }
                        }
                        RespValue::SimpleString(s) => res.push_str(s),
                        RespValue::Integer(i) => res.push_str(&i.to_string()),
                        _ => return None,
                    }
                }
                Some(res)
            }
            _ => None,
        }
    }
}

pub fn decode(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let prefix = buf[0];
    match prefix {
        b'+' => decode_simple_string(buf),
        b'-' => decode_error(buf),
        b':' => decode_integer(buf),
        b'$' => decode_bulk_string(buf),
        b'*' => decode_array(buf),
        _ => {
            // Support simple text/inline commands for backward compatibility and PING
            decode_inline(buf)
        }
    }
}

fn read_line(buf: &mut BytesMut) -> Option<Vec<u8>> {
    for i in 0..buf.len() {
        if i + 1 < buf.len() && buf[i] == b'\r' && buf[i+1] == b'\n' {
            let line = buf.split_to(i).to_vec();
            buf.advance(2); // skip \r\n
            return Some(line);
        }
    }
    None
}

fn decode_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    if let Some(line) = read_line(&mut buf.split_off(1)) {
        buf.advance(line.len() + 3); // '+' + line + \r\n
        let s = String::from_utf8(line)?;
        Ok(Some(RespValue::SimpleString(s)))
    } else {
        Ok(None)
    }
}

fn decode_error(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    if let Some(line) = read_line(&mut buf.split_off(1)) {
        buf.advance(line.len() + 3);
        let s = String::from_utf8(line)?;
        Ok(Some(RespValue::Error(s)))
    } else {
        Ok(None)
    }
}

fn decode_integer(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    if let Some(line) = read_line(&mut buf.split_off(1)) {
        buf.advance(line.len() + 3);
        let s = String::from_utf8(line)?;
        let i = s.parse::<i64>()?;
        Ok(Some(RespValue::Integer(i)))
    } else {
        Ok(None)
    }
}

fn decode_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    // We need to keep the original buffer intact if not enough data
    let mut temp = buf.clone();
    temp.advance(1); // skip '$'
    
    if let Some(line) = read_line(&mut temp) {
        let len = String::from_utf8(line)?.parse::<isize>()?;
        if len == -1 {
            buf.advance(buf.len() - temp.len());
            return Ok(Some(RespValue::BulkString(None)));
        }
        
        let ulen = len as usize;
        if temp.len() >= ulen + 2 {
            let data = temp.split_to(ulen).to_vec();
            if temp[0] != b'\r' || temp[1] != b'\n' {
                return Err(anyhow!("Invalid bulk string terminator"));
            }
            buf.advance(buf.len() - temp.len() + 2);
            Ok(Some(RespValue::BulkString(Some(data))))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn decode_array(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    let mut temp = buf.clone();
    temp.advance(1); // skip '*'
    
    if let Some(line) = read_line(&mut temp) {
        let len = String::from_utf8(line)?.parse::<isize>()?;
        if len == -1 {
            buf.advance(buf.len() - temp.len());
            return Ok(Some(RespValue::Array(None)));
        }
        
        let count = len as usize;
        let mut items = Vec::with_capacity(count);
        
        for _ in 0..count {
            match decode(&mut temp)? {
                Some(val) => items.push(val),
                None => return Ok(None), // Incomplete array
            }
        }
        
        buf.advance(buf.len() - temp.len());
        Ok(Some(RespValue::Array(Some(items))))
    } else {
        Ok(None)
    }
}

fn decode_inline(buf: &mut BytesMut) -> Result<Option<RespValue>> {
    for i in 0..buf.len() {
        if buf[i] == b'\n' {
            let line = buf.split_to(i).to_vec();
            buf.advance(1); // skip \n
            let s = String::from_utf8_lossy(&line).trim().to_string();
            if s.is_empty() { return Ok(None); }
            
            // Convert inline to Array for parser
            let parts: Vec<RespValue> = s.split_whitespace()
                .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
                .collect();
            return Ok(Some(RespValue::Array(Some(parts))));
        }
    }
    Ok(None)
}

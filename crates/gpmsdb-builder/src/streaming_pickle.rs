use std::{
    fs::File,
    io::{BufReader, Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf},
    str::Utf8Error,
};

use thiserror::Error;

#[derive(Debug)]
pub enum StreamError<E> {
    Decode(BuilderError),
    Callback(E),
}

#[derive(Debug, Error)]
pub enum BuilderError {
    #[error("failed to open pickle source {path}: {source}")]
    OpenSource {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed while reading pickle stream at offset {offset}: {source}")]
    ReadSource {
        offset: u64,
        #[source]
        source: IoError,
    },
    #[error("unsupported pickle protocol {version} at offset {offset}; expected protocol 4 or 5")]
    UnsupportedProtocol { offset: u64, version: u8 },
    #[error("unknown opcode 0x{opcode:02x} at offset {offset}")]
    UnknownOpcode { offset: u64, opcode: u8 },
    #[error("invalid UTF-8 for SHORT_BINUNICODE at offset {offset}: {source}")]
    InvalidUtf8 {
        offset: u64,
        #[source]
        source: Utf8Error,
    },
    #[error("unexpected token at offset {offset}: expected {expected}, found {found}")]
    UnexpectedToken {
        offset: u64,
        expected: &'static str,
        found: &'static str,
    },
}

#[derive(Debug, Clone)]
enum Token {
    Proto { offset: u64, version: u8 },
    Frame { offset: u64, len: u64 },
    EmptyDict { offset: u64 },
    Memoize { offset: u64 },
    Mark { offset: u64 },
    ShortBinUnicode { offset: u64, value: String },
    EmptyList { offset: u64 },
    BinFloat { offset: u64, value: f64 },
    Appends { offset: u64 },
    SetItems { offset: u64 },
    Stop { offset: u64 },
}

impl Token {
    fn offset(&self) -> u64 {
        match self {
            Token::Proto { offset, .. }
            | Token::Frame { offset, .. }
            | Token::EmptyDict { offset }
            | Token::Memoize { offset }
            | Token::Mark { offset }
            | Token::ShortBinUnicode { offset, .. }
            | Token::EmptyList { offset }
            | Token::BinFloat { offset, .. }
            | Token::Appends { offset }
            | Token::SetItems { offset }
            | Token::Stop { offset } => *offset,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Token::Proto { .. } => "PROTO",
            Token::Frame { .. } => "FRAME",
            Token::EmptyDict { .. } => "EMPTY_DICT",
            Token::Memoize { .. } => "MEMOIZE",
            Token::Mark { .. } => "MARK",
            Token::ShortBinUnicode { .. } => "SHORT_BINUNICODE",
            Token::EmptyList { .. } => "EMPTY_LIST",
            Token::BinFloat { .. } => "BINFLOAT",
            Token::Appends { .. } => "APPENDS",
            Token::SetItems { .. } => "SETITEMS",
            Token::Stop { .. } => "STOP",
        }
    }
}

pub fn stream_mass_all_db<F>(path: impl AsRef<Path>, on_entry: F) -> Result<(), BuilderError>
where
    F: FnMut(String, Vec<f64>),
{
    let mut on_entry = on_entry;
    try_stream_mass_all_db(path, |genome_id, peaks| {
        on_entry(genome_id, peaks);
        Ok::<(), ()>(())
    })
    .map_err(|error| match error {
        StreamError::Decode(error) => error,
        StreamError::Callback(()) => unreachable!("infallible callback cannot fail"),
    })
}

pub fn try_stream_mass_all_db<F, E>(
    path: impl AsRef<Path>,
    on_entry: F,
) -> Result<(), StreamError<E>>
where
    F: FnMut(String, Vec<f64>) -> Result<(), E>,
{
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|source| BuilderError::OpenSource {
            path: path.to_path_buf(),
            source,
        })
        .map_err(StreamError::Decode)?;

    let reader = PickleReader::new(BufReader::new(file));
    let mut decoder = StreamingDecoder::new(reader);
    decoder.decode_mass_all(on_entry).map_err(StreamError::from)
}

struct StreamingDecoder<R> {
    reader: PickleReader<R>,
    lookahead: Option<Token>,
}

impl<R: Read> StreamingDecoder<R> {
    fn new(reader: PickleReader<R>) -> Self {
        Self {
            reader,
            lookahead: None,
        }
    }

    fn decode_mass_all<F, E>(&mut self, mut on_entry: F) -> Result<(), EOrDecode<E>>
    where
        F: FnMut(String, Vec<f64>) -> Result<(), E>,
    {
        self.expect_proto().map_err(EOrDecode::Decode)?;
        self.expect_empty_dict().map_err(EOrDecode::Decode)?;
        self.consume_memoize_if_present()
            .map_err(EOrDecode::Decode)?;

        loop {
            match self.peek_non_frame().map_err(EOrDecode::Decode)? {
                Token::Mark { .. } => {
                    let _ = self.next_non_frame().map_err(EOrDecode::Decode)?;
                    self.decode_batch(&mut on_entry)?;
                }
                Token::Stop { .. } => {
                    let _ = self.next_non_frame().map_err(EOrDecode::Decode)?;
                    return Ok(());
                }
                token => {
                    return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                        offset: token.offset(),
                        expected: "MARK or STOP",
                        found: token.name(),
                    }));
                }
            }
        }
    }

    fn expect_proto(&mut self) -> Result<(), BuilderError> {
        match self.next_non_frame()? {
            Token::Proto { offset, version } if matches!(version, 4 | 5) => Ok(()),
            Token::Proto { offset, version } => {
                Err(BuilderError::UnsupportedProtocol { offset, version })
            }
            token => Err(BuilderError::UnexpectedToken {
                offset: token.offset(),
                expected: "PROTO",
                found: token.name(),
            }),
        }
    }

    fn expect_empty_dict(&mut self) -> Result<(), BuilderError> {
        match self.next_non_frame()? {
            Token::EmptyDict { .. } => Ok(()),
            token => Err(BuilderError::UnexpectedToken {
                offset: token.offset(),
                expected: "EMPTY_DICT",
                found: token.name(),
            }),
        }
    }

    fn decode_batch<F, E>(&mut self, on_entry: &mut F) -> Result<(), EOrDecode<E>>
    where
        F: FnMut(String, Vec<f64>) -> Result<(), E>,
    {
        loop {
            match self.peek_non_frame().map_err(EOrDecode::Decode)? {
                Token::ShortBinUnicode { .. } => self.decode_entry(on_entry)?,
                Token::SetItems { .. } => {
                    let _ = self.next_non_frame().map_err(EOrDecode::Decode)?;
                    return Ok(());
                }
                token => {
                    return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                        offset: token.offset(),
                        expected: "SHORT_BINUNICODE or SETITEMS",
                        found: token.name(),
                    }));
                }
            }
        }
    }

    fn decode_entry<F, E>(&mut self, on_entry: &mut F) -> Result<(), EOrDecode<E>>
    where
        F: FnMut(String, Vec<f64>) -> Result<(), E>,
    {
        let key = match self.next_non_frame().map_err(EOrDecode::Decode)? {
            Token::ShortBinUnicode { value, .. } => value,
            token => {
                return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                    offset: token.offset(),
                    expected: "SHORT_BINUNICODE",
                    found: token.name(),
                }));
            }
        };
        self.consume_memoize_if_present()
            .map_err(EOrDecode::Decode)?;

        match self.next_non_frame().map_err(EOrDecode::Decode)? {
            Token::EmptyList { .. } => {}
            token => {
                return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                    offset: token.offset(),
                    expected: "EMPTY_LIST",
                    found: token.name(),
                }));
            }
        }
        self.consume_memoize_if_present()
            .map_err(EOrDecode::Decode)?;

        let mut peaks = Vec::new();

        loop {
            match self.next_non_frame().map_err(EOrDecode::Decode)? {
                Token::Mark { .. } => {}
                token => {
                    return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                        offset: token.offset(),
                        expected: "MARK",
                        found: token.name(),
                    }));
                }
            }

            loop {
                match self.next_non_frame().map_err(EOrDecode::Decode)? {
                    Token::BinFloat { value, .. } => peaks.push(value),
                    Token::Appends { .. } => break,
                    token => {
                        return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                            offset: token.offset(),
                            expected: "BINFLOAT or APPENDS",
                            found: token.name(),
                        }));
                    }
                }
            }

            match self.peek_non_frame().map_err(EOrDecode::Decode)? {
                Token::Mark { .. } => continue,
                Token::ShortBinUnicode { .. } | Token::SetItems { .. } | Token::Stop { .. } => {
                    on_entry(key, peaks).map_err(EOrDecode::Callback)?;
                    return Ok(());
                }
                token => {
                    return Err(EOrDecode::Decode(BuilderError::UnexpectedToken {
                        offset: token.offset(),
                        expected: "MARK, SHORT_BINUNICODE, SETITEMS, or STOP",
                        found: token.name(),
                    }));
                }
            }
        }
    }

    fn consume_memoize_if_present(&mut self) -> Result<(), BuilderError> {
        if matches!(self.peek_non_frame()?, Token::Memoize { .. }) {
            let _ = self.next_non_frame()?;
        }
        Ok(())
    }

    fn peek_non_frame(&mut self) -> Result<&Token, BuilderError> {
        loop {
            if self.lookahead.is_none() {
                self.lookahead = Some(self.reader.read_token()?);
            }

            if let Some(Token::Frame { len, .. }) = &self.lookahead {
                let _ = len;
                self.lookahead = None;
                continue;
            }

            return Ok(self.lookahead.as_ref().expect("lookahead populated"));
        }
    }

    fn next_non_frame(&mut self) -> Result<Token, BuilderError> {
        let _ = self.peek_non_frame()?;
        Ok(self.lookahead.take().expect("lookahead populated"))
    }
}

#[derive(Debug)]
enum EOrDecode<E> {
    Decode(BuilderError),
    Callback(E),
}

impl<E> From<EOrDecode<E>> for StreamError<E> {
    fn from(value: EOrDecode<E>) -> Self {
        match value {
            EOrDecode::Decode(error) => StreamError::Decode(error),
            EOrDecode::Callback(error) => StreamError::Callback(error),
        }
    }
}

struct PickleReader<R> {
    inner: R,
    offset: u64,
}

impl<R: Read> PickleReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, offset: 0 }
    }

    fn read_token(&mut self) -> Result<Token, BuilderError> {
        let offset = self.offset;
        let opcode = self.read_u8()?;

        match opcode {
            0x80 => Ok(Token::Proto {
                offset,
                version: self.read_u8()?,
            }),
            0x95 => Ok(Token::Frame {
                offset,
                len: u64::from_le_bytes(self.read_exact::<8>()?),
            }),
            b'}' => Ok(Token::EmptyDict { offset }),
            0x94 => Ok(Token::Memoize { offset }),
            b'(' => Ok(Token::Mark { offset }),
            0x8c => {
                let len = usize::from(self.read_u8()?);
                let bytes = self.read_vec(len)?;
                let value = std::str::from_utf8(&bytes)
                    .map_err(|source| BuilderError::InvalidUtf8 { offset, source })?
                    .to_string();
                Ok(Token::ShortBinUnicode { offset, value })
            }
            b']' => Ok(Token::EmptyList { offset }),
            b'G' => Ok(Token::BinFloat {
                offset,
                value: f64::from_be_bytes(self.read_exact::<8>()?),
            }),
            b'e' => Ok(Token::Appends { offset }),
            b'u' => Ok(Token::SetItems { offset }),
            b'.' => Ok(Token::Stop { offset }),
            _ => Err(BuilderError::UnknownOpcode { offset, opcode }),
        }
    }

    fn read_u8(&mut self) -> Result<u8, BuilderError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_vec(&mut self, len: usize) -> Result<Vec<u8>, BuilderError> {
        let mut bytes = vec![0_u8; len];
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], BuilderError> {
        let mut bytes = [0_u8; N];
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    fn read_into(&mut self, buf: &mut [u8]) -> Result<(), BuilderError> {
        self.inner
            .read_exact(buf)
            .map_err(|source| BuilderError::ReadSource {
                offset: self.offset,
                source: normalize_unexpected_eof(source),
            })?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

fn normalize_unexpected_eof(source: IoError) -> IoError {
    if source.kind() == ErrorKind::UnexpectedEof {
        IoError::new(ErrorKind::UnexpectedEof, "unexpected end of pickle stream")
    } else {
        source
    }
}

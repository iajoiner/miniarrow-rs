// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines `ArrowError` for representing failures in various Arrow operations.
use alloc::string::{String, ToString};
use core::fmt::{Debug, Display, Formatter};

/// Many different operations in the `arrow` crate return this error type.
#[derive(Debug)]
pub enum ArrowError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    CastError(String),
    MemoryError(String),
    ParseError(String),
    SchemaError(String),
    ComputeError(String),
    DivideByZero,
    CsvError(String),
    JsonError(String),
    InvalidArgumentError(String),
    ParquetError(String),
    /// Error during import or export to/from the C Data Interface
    CDataInterface(String),
    DictionaryKeyOverflowError,
    RunEndIndexOverflowError,
}

impl From<alloc::string::FromUtf8Error> for ArrowError {
    fn from(error: alloc::string::FromUtf8Error) -> Self {
        ArrowError::ParseError(error.to_string())
    }
}

impl Display for ArrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            ArrowError::NotYetImplemented(source) => {
                write!(f, "Not yet implemented: {}", &source)
            }
            ArrowError::CastError(desc) => write!(f, "Cast error: {desc}"),
            ArrowError::MemoryError(desc) => write!(f, "Memory error: {desc}"),
            ArrowError::ParseError(desc) => write!(f, "Parser error: {desc}"),
            ArrowError::SchemaError(desc) => write!(f, "Schema error: {desc}"),
            ArrowError::ComputeError(desc) => write!(f, "Compute error: {desc}"),
            ArrowError::DivideByZero => write!(f, "Divide by zero error"),
            ArrowError::CsvError(desc) => write!(f, "Csv error: {desc}"),
            ArrowError::JsonError(desc) => write!(f, "Json error: {desc}"),
            ArrowError::InvalidArgumentError(desc) => {
                write!(f, "Invalid argument error: {desc}")
            }
            ArrowError::ParquetError(desc) => {
                write!(f, "Parquet argument error: {desc}")
            }
            ArrowError::CDataInterface(desc) => {
                write!(f, "C Data interface error: {desc}")
            }
            ArrowError::DictionaryKeyOverflowError => {
                write!(f, "Dictionary key bigger than the key type")
            }
            ArrowError::RunEndIndexOverflowError => {
                write!(f, "Run end encoded array index overflow error")
            }
        }
    }
}

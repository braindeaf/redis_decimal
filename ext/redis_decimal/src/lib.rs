use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue, redis_module};
use bigdecimal::{BigDecimal, Zero};
use std::str::FromStr;

fn parse_decimal(s: &str) -> Result<BigDecimal, RedisError> {
    BigDecimal::from_str(s).map_err(|e| RedisError::String(format!("Invalid decimal: {}", e)))
}

fn round_decimal(val: &BigDecimal) -> BigDecimal {
    val.with_scale(18)
}

fn validate_precision(value: &BigDecimal) -> Result<(), RedisError> {
    let (mantissa, scale) = value.as_bigint_and_exponent();
    let digits = mantissa.to_string().chars().filter(|c| c.is_ascii_digit()).count();
    if scale > 18 {
        return Err(RedisError::String(format!("Too many decimal places: {}", scale)));
    }
    if digits > 52 {
        return Err(RedisError::String(format!("Too many digits: {}", digits)));
    }
    Ok(())
}

fn getdecimal(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }

    let key = &args[1];
    let key_read = ctx.open_key(key);
    match key_read.read()? {
        Some(bytes) => {
            let s = std::str::from_utf8(bytes).map_err(|e| RedisError::String(e.to_string()))?;
            let value = parse_decimal(s)?; // Validate it's a valid decimal
            validate_precision(&value)?;
            Ok(RedisValue::SimpleString(value.to_string()))
        }
        None => Ok(RedisValue::Null),
    }
}

fn setdecimal(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }

    let key = &args[1];
    let input = args[2].try_as_str()?;
    let value = parse_decimal(input)?;

    validate_precision(&value)?;
    let key_write = ctx.open_key_writable(key);
    key_write.write(&value.to_string())?;
    Ok(RedisValue::SimpleString(value.to_string()))
}

fn incrbydecimal(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }

    let key = &args[1];
    let input = args[2].try_as_str()?;
    let increment = parse_decimal(input)?;
    validate_precision(&increment)?;

    // Read existing value atomically
    let current = {
        let key_read = ctx.open_key(key);
        match key_read.read()? {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|e| RedisError::String(e.to_string()))?;
                parse_decimal(s)?
            }
            None => BigDecimal::zero(),
        }
    };

    let result = round_decimal(&(current + increment));
    validate_precision(&result)?;

    let key_write = ctx.open_key_writable(key);
    key_write.write(&result.to_string())?;

    Ok(RedisValue::SimpleString(result.to_string()))
}

fn decrbydecimal(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }

    let key = &args[1];
    let input = args[2].try_as_str()?;
    let decrement = parse_decimal(input)?;
    validate_precision(&decrement)?;

    // Read existing value atomically
    let current = {
        let key_read = ctx.open_key(key);
        match key_read.read()? {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|e| RedisError::String(e.to_string()))?;
                parse_decimal(s)?
            }
            None => BigDecimal::zero(),
        }
    };

    let result = round_decimal(&(current - decrement));
    validate_precision(&result)?;

    let key_write = ctx.open_key_writable(key);
    key_write.write(&result.to_string())?;

    Ok(RedisValue::SimpleString(result.to_string()))
}

// Required metadata for redis_module! macro in 2.0.7
redis_module! {
    name: "redis_decimal",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        ["GETDECIMAL", getdecimal, "", 1, 1, 1],
        ["SETDECIMAL", setdecimal, "", 1, 1, 1],
        ["INCRBYDECIMAL", incrbydecimal, "", 1, 1, 1],
        ["DECRBYDECIMAL", decrbydecimal, "", 1, 1, 1],
    ],
}


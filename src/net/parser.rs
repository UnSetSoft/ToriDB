
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while},
    character::complete::{alpha1, char, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::separated_list1,
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use crate::query::{Command, Operator, Filter, Selector, AlterOp};

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        take_while(|c: char| c.is_alphanumeric() || c == '_')
    ))(input)
}

fn parse_key(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, nom::character::complete::digit1, tag("_"), tag("+"), tag("-"), tag("@"), tag("$"), tag("*"))),
        take_while(|c: char| c.is_alphanumeric() || c == '_' || c == ':' || c == '-' || c == '.' || c == '+' || c == '@' || c == '$' || c == '*')
    ))(input)
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\"')(input)?;
    let mut res = String::new();
    let mut chars = input.char_indices();
    let mut escaped = false;
    let mut end_index = 0;

    while let Some((idx, c)) = chars.next() {
        if escaped {
            match c {
                'n' => res.push('\n'),
                'r' => res.push('\r'),
                't' => res.push('\t'),
                '\"' => res.push('\"'),
                '\\' => res.push('\\'),
                _ => { res.push('\\'); res.push(c); }
            }
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == '\"' {
            end_index = idx + 1;
            break;
        } else {
            res.push(c);
        }
    }
    
    if end_index == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    Ok((&input[end_index..], res))
}

fn parse_string(input: &str) -> IResult<&str, String> {
    alt((
        parse_quoted_string,
        map(parse_key, |s| s.to_string())
    ))(input)
}

fn parse_set(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("SET"),
            multispace1,
            parse_key,
            multispace1,
            parse_string,
        )),
        |(_, _, key, _, value)| Command::Set { key: key.to_string(), value: value.trim().to_string() }
    )(input)
}

// GET key
fn parse_get(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("GET"),
            multispace1,
            parse_key,
        )),
        |(_, _, key)| Command::Get { key: key.to_string() }
    )(input)
}

fn parse_setex(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("SETEX"),
            multispace1,
            parse_key,
            multispace1,
            nom::character::complete::digit1,
            multispace1,
            parse_string,
        )),
        |(_, _, key, _, ttl_str, _, value)| {
            Command::SetEx { 
                key: key.to_string(), 
                value: value.trim().to_string(),
                ttl: ttl_str.parse().unwrap_or(0)
            }
        }
    )(input)
}

// TTL key
fn parse_ttl(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("TTL"),
            multispace1,
            parse_key,
        )),
        |(_, _, key)| Command::Ttl { key: key.to_string() }
    )(input)
}

// AUTH password
fn parse_auth(input: &str) -> IResult<&str, Command> {
    alt((
        map(
            tuple((tag("AUTH"), multispace1, parse_identifier, multispace1, parse_string)),
            |(_, _, user, _, pass)| Command::Auth { username: Some(user.to_string()), password: pass }
        ),
        map(
            tuple((tag("AUTH"), multispace1, parse_string)),
            |(_, _, pass)| Command::Auth { username: None, password: pass }
        ),
    ))(input)
}

// INCR key
fn parse_incr(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("INCR"),
            multispace1,
            parse_key,
        )),
        |(_, _, key)| Command::Incr { key: key.to_string() }
    )(input)
}

// DECR key
fn parse_decr(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("DECR"),
            multispace1,
            parse_key,
        )),
        |(_, _, key)| Command::Decr { key: key.to_string() }
    )(input)
}

// CREATE TABLE name (col1 type [PK], col2 type)
// Syntax: CREATE TABLE name col:type[:pk] col:type ...
fn parse_create_table(input: &str) -> IResult<&str, Command> {
    let parse_col_def = map(
        tuple((
            parse_identifier,
            char(':'),
            parse_identifier,
            // Optional :pk
            opt(preceded(char(':'), alt((tag_no_case("pk"), tag_no_case("primary key"))))),
            // Optional :fk(table.col)
            opt(preceded(
                tuple((char(':'), tag_no_case("fk"))), 
                delimited(
                    char('('), 
                    pair(parse_identifier, preceded(char('.'), parse_identifier)),
                    char(')')
                )
            ))
        )),
        |(name, _, dtype, pk, fk)| (
            name.to_string(), 
            dtype.to_string(), 
            pk.is_some(), 
            fk.map(|(t, c)| (t.to_string(), c.to_string()))
        )
    );

    map(
        tuple((
            tag_no_case("CREATE"),
            multispace1,
            tag_no_case("TABLE"),
            multispace1,
            parse_identifier,
            multispace1,
            separated_list1(multispace1, parse_col_def)
        )),
        |(_, _, _, _, name, _, columns)| Command::CreateTable { name: name.to_string(), columns }
    )(input)
}

// ALTER TABLE name ADD/DROP ...
fn parse_alter_table(input: &str) -> IResult<&str, Command> {
    let parse_add = map(
        tuple((
            tag_no_case("ADD"),
            multispace1,
            parse_identifier,
            char(':'),
            parse_identifier
        )),
        |(_, _, col, _, dtype)| AlterOp::Add(col.to_string(), dtype.to_string())
    );

    let parse_drop = map(
        tuple((
            tag_no_case("DROP"),
            multispace1,
            parse_identifier
        )),
        |(_, _, col)| AlterOp::Drop(col.to_string())
    );

    map(
        tuple((
            tag_no_case("ALTER"),
            multispace1,
            tag_no_case("TABLE"),
            multispace1,
            parse_identifier,
            multispace1,
            alt((parse_add, parse_drop))
        )),
        |(_, _, _, _, table, _, op)| Command::AlterTable { table: table.to_string(), op }
    )(input)
}

// --- LISTS ---
// LPUSH key val1 val2 ...
fn parse_lpush(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("LPUSH"),
            multispace1,
            parse_key,
            multispace1,
            separated_list1(multispace1, parse_string)
        )),
        |(_, _, key, _, values)| Command::LPush { key: key.to_string(), values }
    )(input)
}

// RPUSH key val1 val2 ...
fn parse_rpush(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("RPUSH"),
            multispace1,
            parse_key,
            multispace1,
            separated_list1(multispace1, parse_string)
        )),
        |(_, _, key, _, values)| Command::RPush { key: key.to_string(), values }
    )(input)
}

// LPOP key [count]
fn parse_lpop(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("LPOP"),
            multispace1,
            parse_key,
            opt(preceded(multispace1, nom::character::complete::u64)) // Optional count
        )),
        |(_, _, key, count)| Command::LPop { key: key.to_string(), count: count.map(|c| c as usize) }
    )(input)
}

// RPOP key [count]
fn parse_rpop(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("RPOP"),
            multispace1,
            parse_key,
            opt(preceded(multispace1, nom::character::complete::u64))
        )),
        |(_, _, key, count)| Command::RPop { key: key.to_string(), count: count.map(|c| c as usize) }
    )(input)
}

// LRANGE key start stop
fn parse_lrange(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("LRANGE"),
            multispace1,
            parse_key,
            multispace1,
            nom::character::complete::i64,
            multispace1,
            nom::character::complete::i64
        )),
        |(_, _, key, _, start, _, stop)| Command::LRange { key: key.to_string(), start, stop }
    )(input)
}

// --- HASHES ---
// HSET key field value
fn parse_hset(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("HSET"),
            multispace1,
            parse_key,
            multispace1,
            parse_string, // Field
            multispace1,
            parse_string  // Value
        )),
        |(_, _, key, _, field, _, value)| Command::HSet { key: key.to_string(), field, value }
    )(input)
}

// HGET key field
fn parse_hget(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("HGET"),
            multispace1,
            parse_key,
            multispace1,
            parse_string
        )),
        |(_, _, key, _, field)| Command::HGet { key: key.to_string(), field }
    )(input)
}

// HGETALL key
fn parse_hgetall(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("HGETALL"),
            multispace1,
            parse_key
        )),
        |(_, _, key)| Command::HGetAll { key: key.to_string() }
    )(input)
}

// --- SETS ---
// SADD key val1 val2 ...
fn parse_sadd(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("SADD"),
            multispace1,
            parse_key,
            multispace1,
            separated_list1(multispace1, parse_string)
        )),
        |(_, _, key, _, values)| Command::SAdd { key: key.to_string(), members: values }
    )(input)
}

// SMEMBERS key
fn parse_smembers(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("SMEMBERS"),
            multispace1,
            parse_key
        )),
        |(_, _, key)| Command::SMembers { key: key.to_string() }
    )(input)
}

// --- JSON ---
// JSON.GET key [path]
fn parse_json_get(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("JSON.GET"),
            multispace1,
            parse_key,
            opt(preceded(multispace1, parse_string)) // Optional path
        )),
        |(_, _, key, path)| Command::JsonGet { key: key.to_string(), path }
    )(input)
}

// JSON.SET key path value
fn parse_json_set(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag_no_case("JSON.SET"),
            multispace1,
            parse_key,
            multispace1,
            parse_string, // Path
            multispace1,
            parse_string  // Value (stringified JSON)
        )),
        |(_, _, key, _, path, _, value)| Command::JsonSet { key: key.to_string(), path, value }
    )(input)
}

// INSERT INTO table (val1, val2) -> Simplified: INSERT table val1 val2
fn parse_insert(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("INSERT"),
            multispace1,
            parse_identifier,
            multispace1,
            separated_list1(multispace1, parse_string)
        )),
        |(_, _, table, _, values)| Command::Insert { table: table.to_string(), values }
    )(input)
}



// ... existing imports ...



// ... existing imports ...

fn parse_operator(input: &str) -> IResult<&str, Operator> {
    alt((
        map(tag("LIKE"), |_| Operator::Like),
        map(tag("IN"), |_| Operator::In),
        map(tag("="), |_| Operator::Eq),
        map(tag("!="), |_| Operator::Neq),
        map(tag(">="), |_| Operator::Gte),
        map(tag("<="), |_| Operator::Lte),
        map(tag(">"), |_| Operator::Gt),
        map(tag("<"), |_| Operator::Lt),
    ))(input)
}

// Helper to parse a list of values: (val1, val2, ...)
fn parse_value_list(input: &str) -> IResult<&str, String> {
    delimited(
        char('('),
        map(
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_string
            ),
            |vals| vals.join(",")
        ),
        char(')')
    )(input)
}

// Atom: col op val
fn parse_condition(input: &str) -> IResult<&str, Filter> {
    map(
        tuple((
            parse_identifier,
            multispace1,
            parse_operator,
            multispace1,
            alt((
                parse_value_list, // Try parsing list first for IN
                parse_string
            )),
        )),
        |(col, _, op, _, val)| Filter::Condition(col.to_string(), op, val)
    )(input)
}

fn parse_atom(input: &str) -> IResult<&str, Filter> {
    alt((
        delimited(
            tuple((char('('), multispace0)),
            parse_filter,
            tuple((multispace0, char(')')))
        ),
        parse_condition,
    ))(input)
}

// Term: Atom AND Atom AND ...
fn parse_and_term(input: &str) -> IResult<&str, Filter> {
    let (input, first) = parse_atom(input)?;
    let (input, rest) = nom::multi::fold_many0(
        preceded(tuple((multispace1, tag("AND"), multispace1)), parse_atom),
        move || first.clone(),
        |acc, val| Filter::And(Box::new(acc), Box::new(val))
    )(input)?;
    Ok((input, rest))
}

// Expr: Term OR Term OR ...
fn parse_filter(input: &str) -> IResult<&str, Filter> {
    let (input, first) = parse_and_term(input)?;
    let (input, rest) = nom::multi::fold_many0(
        preceded(tuple((multispace1, tag("OR"), multispace1)), parse_and_term),
        move || first.clone(),
        |acc, val| Filter::Or(Box::new(acc), Box::new(val))
    )(input)?;
    Ok((input, rest))
}

// ZSET Commands
fn parse_zadd(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag_no_case("ZADD"), multispace1, parse_key, multispace1, nom::number::complete::double, multispace1, parse_string)),
        |(_, _, key, _, score, _, member)| Command::ZAdd { key: key.to_string(), score, member }
    )(input)
}

fn parse_zrange(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag_no_case("ZRANGE"), multispace1, parse_key, multispace1, nom::character::complete::i64, multispace1, nom::character::complete::i64)),
        |(_, _, key, _, start, _, stop)| Command::ZRange { key: key.to_string(), start, stop }
    )(input)
}

fn parse_zscore(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag_no_case("ZSCORE"), multispace1, parse_key, multispace1, parse_string)),
        |(_, _, key, _, member)| Command::ZScore { key: key.to_string(), member }
    )(input)
}

fn parse_ping(input: &str) -> IResult<&str, Command> {
    map(tag("PING"), |_| Command::Ping)(input)
}

fn parse_save(input: &str) -> IResult<&str, Command> {
    map(tag("SAVE"), |_| Command::Save)(input)
}

// UPDATE table SET col=val [WHERE filter]
fn parse_update(input: &str) -> IResult<&str, Command> {
    let parse_where = preceded(
        tuple((multispace1, tag("WHERE"), multispace1)),
        parse_filter
    );

    map(
        tuple((
            tag("UPDATE"),
            multispace1,
            parse_identifier,
            multispace1,
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace1,
            char('='),
            multispace1,
            parse_string,
            opt(parse_where)
        )),
        |(_, _, table, _, _, _, set_col, _, _, _, set_val, filter)| {
            Command::Update {
                table: table.to_string(),
                filter, // Now Option<Filter>
                set: (set_col.to_string(), set_val),
            }
        }
    )(input)
}

// DELETE FROM table [WHERE filter]
fn parse_delete(input: &str) -> IResult<&str, Command> {
    let parse_where = preceded(
        tuple((multispace1, tag("WHERE"), multispace1)),
        parse_filter
    );
    
    map(
        tuple((
            tag("DELETE"),
            multispace1,
            tag("FROM"),
            multispace1,
            parse_identifier,
            opt(parse_where)
        )),
        |(_, _, _, _, table, filter)| {
            Command::Delete {
                table: table.to_string(),
                filter,
            }
        }
    )(input)
}

// SELECT [COUNT(*) | *] FROM table [WHERE...] [ORDER BY col [ASC|DESC]] [LIMIT n]
fn parse_select(input: &str) -> IResult<&str, Command> {
    // Legacy: SELECT table [WHERE...]
    let parse_where_legacy = preceded(
        tuple((multispace1, tag("WHERE"), multispace1)),
        parse_filter
    );

    let parse_legacy_select = map(
        tuple((
            tag("SELECT"),
            multispace1,
            parse_identifier,
            opt(parse_where_legacy)
        )),
        |(_, _, table, filter)| {
             Command::Select { 
                 table: table.to_string(), 
                 selector: Selector::All, 
                 filter, 
                 group_by: None,
                 having: None,
                 order_by: None, 
                 limit: None,
                 offset: None,
            }
        }
    );

    // Full: SELECT selector FROM table [WHERE...] [ORDER BY...] [LIMIT...]
    let parse_selector = alt((
        map(alt((tag("COUNT(*)"), tag("COUNT"), tag("count(*)"), tag("count"))), |_| Selector::Count),
        map(
            delimited(tag("SUM("), parse_identifier, char(')')),
            |col| Selector::Sum(col.to_string())
        ),
        map(
            delimited(tag("AVG("), parse_identifier, char(')')),
            |col| Selector::Avg(col.to_string())
        ),
        map(
            delimited(tag("MAX("), parse_identifier, char(')')),
            |col| Selector::Max(col.to_string())
        ),
        map(
            delimited(tag("MIN("), parse_identifier, char(')')),
            |col| Selector::Min(col.to_string())
        ),
        map(tag("*"), |_| Selector::All),
    ));

    let parse_where = preceded(
        tuple((multispace1, tag("WHERE"), multispace1)),
        parse_filter
    );

    let parse_group_by = preceded(
        tuple((multispace1, tag("GROUP"), multispace1, tag("BY"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)), 
            parse_identifier
        )
    );

    let parse_having = preceded(
        tuple((multispace1, tag("HAVING"), multispace1)),
        parse_filter
    );

    let parse_order_by = preceded(
        tuple((multispace1, tag("ORDER"), multispace1, tag("BY"), multispace1)),
        pair(
            parse_identifier,
            opt(preceded(multispace1, alt((tag("ASC"), tag("DESC")))))
        )
    );

    let parse_limit = preceded(
        tuple((multispace1, tag("LIMIT"), multispace1)),
        nom::character::complete::digit1
    );

    let parse_offset = preceded(
        tuple((multispace1, tag("OFFSET"), multispace1)),
        nom::character::complete::digit1
    );

    let parse_full_select = map(
        tuple((
            tag("SELECT"),
            multispace1,
            parse_selector,
            multispace1,
            tag("FROM"),
            multispace1,
            parse_identifier,
            opt(parse_where),
            opt(parse_group_by),
            opt(parse_having),
            opt(parse_order_by),
            opt(parse_limit),
            opt(parse_offset)
        )),
        |(_, _, selector, _, _, _, table, filter, group_by, having, order, limit_str, offset_str)| {
            let group_by = group_by.map(|cols: Vec<&str>| cols.iter().map(|s| s.to_string()).collect());
            let order_by = order.map(|(col, dir)| {
                (col.to_string(), dir.unwrap_or("ASC") == "ASC")
            });
            let limit = limit_str.and_then(|s| s.parse::<usize>().ok());
            let offset = offset_str.and_then(|s| s.parse::<usize>().ok());
            
            Command::Select {
                table: table.to_string(),
                selector,
                filter,
                group_by,
                having,
                order_by,
                limit,
                offset
            }
        }
    );

    alt((parse_full_select, parse_legacy_select))(input)
}

// CREATE INDEX idx ON table(col)
fn parse_create_index(input: &str) -> IResult<&str, Command> {
    map(
        tuple((
            tag("CREATE"),
            multispace1,
            tag("INDEX"),
            multispace1,
            parse_identifier,
            multispace1,
            tag("ON"),
            multispace1,
            parse_identifier,
            char('('),
            parse_identifier,
            char(')')
        )),
        |(_, _, _, _, idx_name, _, _, _, table, _, col, _)| {
            Command::CreateIndex {
                index_name: idx_name.to_string(),
                table: table.to_string(),
                column: col.to_string(),
            }
        }
    )(input)
}

fn parse_acl(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("ACL")(input)?;
    let (input, _) = multispace1(input)?;
    
    alt((
        map(
            tuple((tag_no_case("SETUSER"), multispace1, parse_identifier, multispace1, parse_string, multispace1, separated_list1(multispace1, parse_string))),
            |(_, _, username, _, password, _, rules)| Command::AclSetUser { username: username.to_string(), password, rules }
        ),
        map(
            tuple((tag_no_case("GETUSER"), multispace1, parse_identifier)),
            |(_, _, username)| Command::AclGetUser { username: username.to_string() }
        ),
        map(tag_no_case("LIST"), |_| Command::AclList),
        map(
            tuple((tag_no_case("DELUSER"), multispace1, parse_identifier)),
            |(_, _, username)| Command::AclDelUser { username: username.to_string() }
        ),
    ))(input)
}

fn parse_rewrite_aof(input: &str) -> IResult<&str, Command> {
    map(
        alt((tag_no_case("REWRITEAOF"), tag_no_case("BGREWRITEAOF"))),
        |_| Command::RewriteAof
    )(input)
}

fn parse_use(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag_no_case("USE"), multispace1, parse_identifier)),
        |(_, _, db)| Command::Use { db_name: db.to_string() }
    )(input)
}

fn parse_client(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_no_case("CLIENT")(input)?;
    let (input, _) = multispace1(input)?;
    
    alt((
        map(tag_no_case("LIST"), |_| Command::ClientList),
        map(
            tuple((tag_no_case("KILL"), multispace1, parse_string)),
            |(_, _, addr)| Command::ClientKill { addr }
        ),
    ))(input)
}

fn parse_replicaof(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag_no_case("REPLICAOF"), multispace1, parse_string, multispace1, parse_string)),
        |(_, _, host, _, port)| Command::ReplicaOf { host, port }
    )(input)
}

fn parse_psync(input: &str) -> IResult<&str, Command> {
    map(
        tag_no_case("PSYNC"),
        |_| Command::Psync
    )(input)
}

fn parse_info(input: &str) -> IResult<&str, Command> {
    map(
        tag_no_case("INFO"),
        |_| Command::Info
    )(input)
}

fn parse_cluster(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_no_case("CLUSTER")(input)?;
    let (input, _) = multispace1(input)?;
    
    alt((
        map(tag_no_case("INFO"), |_| Command::ClusterInfo),
        map(tag_no_case("SLOTS"), |_| Command::ClusterSlots),
        map(
            tuple((tag_no_case("MEET"), multispace1, parse_string, multispace1, parse_string)),
            |(_, _, host, _, port)| Command::ClusterMeet { host, port: port.parse().unwrap_or(0) }
        ),
        map(
            tuple((tag_no_case("ADDSLOTS"), multispace1, separated_list1(multispace1, nom::character::complete::u16))),
            |(_, _, slots)| Command::ClusterAddSlots { slots }
        ),
    ))(input)
}

pub fn parse_command(input: &str) -> IResult<&str, Command> {
    let (remaining, _) = multispace0(input)?;
    
    // Try main commands first
    // Group 1: Core KV & Security
    if let Ok(result) = alt((
        parse_setex,
        parse_set,
        parse_get,
        parse_ttl,
        parse_auth,
        parse_acl,
        parse_incr,
        parse_decr,
        parse_use,
        parse_rewrite_aof,
        parse_ping,
        parse_save,
        parse_client,
        parse_replicaof,
        parse_psync,
        parse_info,
        parse_cluster,
    ))(remaining) {
        return Ok(result);
    }

    // Group 2: Structured (SQL-like)
    if let Ok(result) = alt((
        parse_create_index,
        parse_create_table,
        parse_alter_table,
        parse_insert,
        parse_select,
        parse_update,
        parse_delete,
    ))(remaining) {
        return Ok(result);
    }

    // Group 3: Flexible (Lists, Hashes, Sets, JSON, ZSET)
    if let Ok(result) = alt((
        parse_lpush, parse_rpush, parse_lpop, parse_rpop, parse_lrange,
        parse_hset, parse_hget, parse_hgetall,
        parse_sadd, parse_smembers,
        parse_zadd, parse_zrange, parse_zscore,
        parse_json_get, parse_json_set,
    ))(remaining) {
        return Ok(result);
    }
    
    // Fallback or explicit error
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
}

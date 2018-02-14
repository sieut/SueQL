extern crate regex;
use self::regex::{Regex, RegexSet};

#[derive(PartialEq, PartialOrd, Clone, Debug)]
pub enum Token {
    Create, Drop, Insert, Delete, Select, Update,
    Into, Set, From, Where, Values,
    Table, Database,

    LeftParen, RightParen,
    Comma, Dot, Semi,

    Number(String), StringVal(String),

    Null, Identifier(String),
}

impl Token {
    /// Return a tuple (token, lexem_len) with the token at the beginning of input
    fn string_to_token(input: &str) -> Option<(Token, usize)> {
        lazy_static! {
            static ref REG: Vec<Regex> = vec![
                Regex::new(r"create").unwrap(),
                Regex::new(r"drop").unwrap(),
                Regex::new(r"insert").unwrap(),
                Regex::new(r"delete").unwrap(),
                Regex::new(r"select").unwrap(),
                Regex::new(r"update").unwrap(),
                Regex::new(r"into").unwrap(),
                Regex::new(r"set").unwrap(),
                Regex::new(r"from").unwrap(),
                Regex::new(r"where").unwrap(),
                Regex::new(r"values").unwrap(),
                Regex::new(r"table").unwrap(),
                Regex::new(r"database").unwrap(),
                Regex::new(r"\(").unwrap(),
                Regex::new(r"\)").unwrap(),
                Regex::new(r",").unwrap(),
                Regex::new(r"\.").unwrap(),
                Regex::new(r";").unwrap(),
                Regex::new(r"[0-9]*(\.[0-9]+)?").unwrap(),
                Regex::new(r"('')|('[^\x00]*?[^\\]')").unwrap(),        // The string regex is messy
                Regex::new(r"null").unwrap(),
                Regex::new(r"[a-zA-Z_][a-zA-Z_0-9]*").unwrap(),
            ];

            static ref TOKEN_IDX: Vec<Token> = vec![
                Token::Create, Token::Drop, Token::Insert, Token::Delete, Token::Select, Token::Update,
                Token::Into, Token::Set, Token::From, Token::Where, Token::Values,
                Token::Table, Token::Database,

                Token::LeftParen, Token::RightParen,
                Token::Comma, Token::Dot, Token::Semi,

                Token::Number(String::from("")), Token::StringVal(String::from("")),

                Token::Null, Token::Identifier(String::from("")),
            ];
        }

        let mut ret: Option<(Token, usize)> = None;
        let mut max_len: usize = 0;
        for (reg_idx, reg) in REG.iter().enumerate() {
            match (*reg).find(input) {
                Some(mat) => {
                    if mat.start() == 0 && mat.end() - mat.start() > max_len {
                        max_len = mat.end() - mat.start();
                        ret = Some((TOKEN_IDX[reg_idx].clone(), max_len));

                        match ret.clone().unwrap() {
                            (Token::Identifier(_), _) => ret = Some(( Token::Identifier(String::from(&input[mat.start()..mat.end()])), max_len )),
                            (Token::Number(_), _) => ret = Some(( Token::Number(String::from(&input[mat.start()..mat.end()])), max_len )),
                            (Token::StringVal(_), _) => ret = Some(( Token::StringVal(String::from(&input[mat.start()+1..mat.end()-1])), max_len )),
                            _ => {}
                        }
                    }
                }
                None => {}
            }
        }

        ret
    }
}

pub struct Lexer {
    input: String,
}

impl Lexer {
    pub fn new(input: &String) -> Lexer {
        // Not supporting UTF-8 for now
        assert!(input.is_ascii());
        Lexer { input: input.to_lowercase() }
    }

    pub fn lex(&mut self) -> Option<Vec<Token>> {
        let mut tokens: Vec<Token> = vec![];
        let mut remaining_input = self.input.as_str();

        while remaining_input.len() > 0 {
            let first_char: char = remaining_input.chars().next().unwrap();
            match first_char {
                // Ignored characters
                ' ' | '\n' | '\t' => {
                    remaining_input = remaining_input.split_at(1).1;
                    continue;
                }
                _ => {}
            }

            match Token::string_to_token(&remaining_input) {
                Some((token, lexeme_len)) => {
                    tokens.push(token);
                    remaining_input = remaining_input.split_at(lexeme_len).1;
                }
                None => {
                    self.error();
                    return None;
                }
            }
        }

        Some(tokens)
    }

    fn error(&self) {
    }
}

#[cfg(test)]
mod tests {
    use sql::lexer::{Lexer, Token};

    #[test]
    fn test_string_to_tokens() {
        let id_1 = Token::string_to_token("column_name");
        assert_eq!(id_1.unwrap().0, Token::Identifier(String::from("column_name")));
        let id_2 = Token::string_to_token("create_table123");
        assert_eq!(id_2.unwrap().0, Token::Identifier(String::from("create_table123")));

        let create = Token::string_to_token("create table");
        assert_eq!(create.unwrap().0, Token::Create);

        let num_1 = Token::string_to_token("00.12something else");
        assert_eq!(num_1.unwrap().0, Token::Number(String::from("00.12")));
        let num_2 = Token::string_to_token(".12 ");
        assert_eq!(num_2.unwrap().0, Token::Number(String::from(".12")));
        let num_3 = Token::string_to_token("6969");
        assert_eq!(num_3.unwrap().0, Token::Number(String::from("6969")));

        let string_1 = Token::string_to_token("'just an innocent\\t string'");
        assert_eq!(string_1.unwrap().0, Token::StringVal(String::from("just an innocent\\t string")));
        let string_2 = Token::string_to_token("'this string span\nover line'");
        assert_eq!(string_2.unwrap().0, Token::StringVal(String::from("this string span\nover line")));
        let string_3 = Token::string_to_token("'this string is invalid \\'");
        assert!(string_3.is_none());
        let string_4 = Token::string_to_token("''");
        assert_eq!(string_4.unwrap().0, Token::StringVal(String::from("")));
        let string_5 = Token::string_to_token("'2 strings in 1 line' + 'the second string'");
        assert_eq!(string_5.unwrap().0, Token::StringVal(String::from("2 strings in 1 line")));
    }

    #[test]
    fn test_lexer_simple_create() {
        let input:String = String::from("CREATE TABLE table_name420;");
        let mut lexer = Lexer::new(&input);
        let tokens = lexer.lex().unwrap();

        assert_eq!(tokens.len(), 4);
        let mut tok_iter = tokens.iter();
        assert_eq!(tok_iter.next().unwrap(), &Token::Create);
        assert_eq!(tok_iter.next().unwrap(), &Token::Table);
        assert_eq!(tok_iter.next().unwrap(), &Token::Identifier(String::from("table_name420")));
        assert_eq!(tok_iter.next().unwrap(), &Token::Semi);
    }

    #[test]
    fn test_lexer_simple_insert() {
        let input:String = String::from(" INSERT INTO table_name (column_2, column_4) VALUES (12.5, 'customer_name'); ");
        let mut lexer = Lexer::new(&input);
        let tokens = lexer.lex().unwrap();

        assert_eq!(tokens.len(), 15);
        let mut tok_iter = tokens.iter();
        assert_eq!(tok_iter.next().unwrap(), &Token::Insert);
        assert_eq!(tok_iter.next().unwrap(), &Token::Into);
        assert_eq!(tok_iter.next().unwrap(), &Token::Identifier(String::from("table_name")));
        assert_eq!(tok_iter.next().unwrap(), &Token::LeftParen);
        assert_eq!(tok_iter.next().unwrap(), &Token::Identifier(String::from("column_2")));
        assert_eq!(tok_iter.next().unwrap(), &Token::Comma);
        assert_eq!(tok_iter.next().unwrap(), &Token::Identifier(String::from("column_4")));
        assert_eq!(tok_iter.next().unwrap(), &Token::RightParen);
        assert_eq!(tok_iter.next().unwrap(), &Token::Values);
        assert_eq!(tok_iter.next().unwrap(), &Token::LeftParen);
        assert_eq!(tok_iter.next().unwrap(), &Token::Number(String::from("12.5")));
        assert_eq!(tok_iter.next().unwrap(), &Token::Comma);
        assert_eq!(tok_iter.next().unwrap(), &Token::StringVal(String::from("customer_name")));
        assert_eq!(tok_iter.next().unwrap(), &Token::RightParen);
        assert_eq!(tok_iter.next().unwrap(), &Token::Semi);
    }
}

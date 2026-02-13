const std = @import("std");

/// Token types for YQL lexer
pub const TokenType = enum {
    // Literals
    identifier,
    string,
    number,

    // Operators
    dot, // .
    lparen, // (
    rparen, // )
    lbrace, // {
    rbrace, // }
    lbracket, // [
    rbracket, // ]
    comma, // ,
    colon, // :
    eq, // =
    ne, // !=
    gt, // >
    gte, // >=
    lt, // <
    lte, // <=
    tilde, // ~ (regex)

    // Keywords
    kw_and,
    kw_or,
    kw_not,
    kw_in,
    kw_contains,
    kw_starts_with,
    kw_exists,
    kw_true,
    kw_false,
    kw_null,
    kw_asc,
    kw_desc,
    kw_count,
    kw_sum,
    kw_avg,
    kw_min,
    kw_max,

    // Special
    eof,
    invalid,
};

/// A token from the lexer
pub const Token = struct {
    type: TokenType,
    text: []const u8,
    line: u32,
    col: u32,

    pub fn isKeyword(self: Token) bool {
        return switch (self.type) {
            .kw_and, .kw_or, .kw_not, .kw_in, .kw_contains, .kw_starts_with, .kw_exists, .kw_true, .kw_false, .kw_null, .kw_asc, .kw_desc, .kw_count, .kw_sum, .kw_avg, .kw_min, .kw_max => true,
            else => false,
        };
    }

    pub fn isOperator(self: Token) bool {
        return switch (self.type) {
            .eq, .ne, .gt, .gte, .lt, .lte, .tilde => true,
            else => false,
        };
    }
};

/// Lexer for YQL text
pub const Lexer = struct {
    source: []const u8,
    pos: usize,
    line: u32,
    col: u32,

    pub fn init(source: []const u8) Lexer {
        return .{
            .source = source,
            .pos = 0,
            .line = 1,
            .col = 1,
        };
    }

    /// Get the next token
    pub fn next(self: *Lexer) Token {
        self.skipWhitespace();

        if (self.isAtEnd()) {
            return self.makeToken(.eof, "");
        }

        const start_col = self.col;
        const c = self.advance();

        // Single character tokens
        switch (c) {
            '.' => return self.makeToken(.dot, "."),
            '(' => return self.makeToken(.lparen, "("),
            ')' => return self.makeToken(.rparen, ")"),
            '{' => return self.makeToken(.lbrace, "{"),
            '}' => return self.makeToken(.rbrace, "}"),
            '[' => return self.makeToken(.lbracket, "["),
            ']' => return self.makeToken(.rbracket, "]"),
            ',' => return self.makeToken(.comma, ","),
            ':' => return self.makeToken(.colon, ":"),
            '~' => return self.makeToken(.tilde, "~"),
            '=' => return self.makeToken(.eq, "="),
            '!' => {
                if (self.match('=')) {
                    return self.makeToken(.ne, "!=");
                }
                return self.makeTokenAt(.invalid, "!", start_col);
            },
            '>' => {
                if (self.match('=')) {
                    return self.makeToken(.gte, ">=");
                }
                return self.makeToken(.gt, ">");
            },
            '<' => {
                if (self.match('=')) {
                    return self.makeToken(.lte, "<=");
                }
                return self.makeToken(.lt, "<");
            },
            '"', '\'' => return self.scanString(c),
            else => {},
        }

        // Numbers
        if (isDigit(c) or (c == '-' and self.peekChar() != null and isDigit(self.peekChar().?))) {
            return self.scanNumber(start_col);
        }

        // Identifiers and keywords
        if (isAlpha(c) or c == '_') {
            return self.scanIdentifier(start_col);
        }

        return self.makeTokenAt(.invalid, self.source[self.pos - 1 .. self.pos], start_col);
    }

    /// Peek at the current token without consuming
    pub fn peek(self: *Lexer) Token {
        const saved_pos = self.pos;
        const saved_line = self.line;
        const saved_col = self.col;

        const token = self.next();

        self.pos = saved_pos;
        self.line = saved_line;
        self.col = saved_col;

        return token;
    }

    // === Private helpers ===

    fn isAtEnd(self: *const Lexer) bool {
        return self.pos >= self.source.len;
    }

    fn advance(self: *Lexer) u8 {
        const c = self.source[self.pos];
        self.pos += 1;
        if (c == '\n') {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        return c;
    }

    fn peekChar(self: *const Lexer) ?u8 {
        if (self.isAtEnd()) return null;
        return self.source[self.pos];
    }

    fn match(self: *Lexer, expected: u8) bool {
        if (self.isAtEnd()) return false;
        if (self.source[self.pos] != expected) return false;
        _ = self.advance();
        return true;
    }

    fn skipWhitespace(self: *Lexer) void {
        while (!self.isAtEnd()) {
            const c = self.source[self.pos];
            switch (c) {
                ' ', '\t', '\r', '\n' => _ = self.advance(),
                else => return,
            }
        }
    }

    fn makeToken(self: *const Lexer, token_type: TokenType, text: []const u8) Token {
        return .{
            .type = token_type,
            .text = text,
            .line = self.line,
            .col = self.col -| @as(u32, @intCast(text.len)),
        };
    }

    fn makeTokenAt(self: *const Lexer, token_type: TokenType, text: []const u8, col: u32) Token {
        return .{
            .type = token_type,
            .text = text,
            .line = self.line,
            .col = col,
        };
    }

    fn scanString(self: *Lexer, quote: u8) Token {
        const start = self.pos;
        const start_col = self.col - 1;

        while (!self.isAtEnd() and self.source[self.pos] != quote) {
            if (self.source[self.pos] == '\\' and self.pos + 1 < self.source.len) {
                _ = self.advance(); // skip escape char
            }
            _ = self.advance();
        }

        if (self.isAtEnd()) {
            return self.makeTokenAt(.invalid, self.source[start - 1 ..], start_col);
        }

        const text = self.source[start..self.pos];
        _ = self.advance(); // closing quote

        return self.makeTokenAt(.string, text, start_col);
    }

    fn scanNumber(self: *Lexer, start_col: u32) Token {
        const start = self.pos - 1;
        var has_dot = false;

        while (!self.isAtEnd()) {
            const c = self.source[self.pos];
            if (isDigit(c)) {
                _ = self.advance();
            } else if (c == '.' and !has_dot) {
                // Check if next char is digit (not a method call)
                if (self.pos + 1 < self.source.len and isDigit(self.source[self.pos + 1])) {
                    has_dot = true;
                    _ = self.advance();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        return self.makeTokenAt(.number, self.source[start..self.pos], start_col);
    }

    fn scanIdentifier(self: *Lexer, start_col: u32) Token {
        const start = self.pos - 1;

        while (!self.isAtEnd()) {
            const c = self.source[self.pos];
            if (isAlphaNumeric(c) or c == '_') {
                _ = self.advance();
            } else {
                break;
            }
        }

        const text = self.source[start..self.pos];
        const token_type = identifierType(text);

        return self.makeTokenAt(token_type, text, start_col);
    }
};

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

fn isAlpha(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}

fn isAlphaNumeric(c: u8) bool {
    return isAlpha(c) or isDigit(c);
}

fn identifierType(text: []const u8) TokenType {
    // Keywords
    if (std.mem.eql(u8, text, "and")) return .kw_and;
    if (std.mem.eql(u8, text, "or")) return .kw_or;
    if (std.mem.eql(u8, text, "not")) return .kw_not;
    if (std.mem.eql(u8, text, "in")) return .kw_in;
    if (std.mem.eql(u8, text, "contains")) return .kw_contains;
    if (std.mem.eql(u8, text, "startsWith")) return .kw_starts_with;
    if (std.mem.eql(u8, text, "exists")) return .kw_exists;
    if (std.mem.eql(u8, text, "true")) return .kw_true;
    if (std.mem.eql(u8, text, "false")) return .kw_false;
    if (std.mem.eql(u8, text, "null")) return .kw_null;
    if (std.mem.eql(u8, text, "asc")) return .kw_asc;
    if (std.mem.eql(u8, text, "desc")) return .kw_desc;
    if (std.mem.eql(u8, text, "count")) return .kw_count;
    if (std.mem.eql(u8, text, "sum")) return .kw_sum;
    if (std.mem.eql(u8, text, "avg")) return .kw_avg;
    if (std.mem.eql(u8, text, "min")) return .kw_min;
    if (std.mem.eql(u8, text, "max")) return .kw_max;

    return .identifier;
}

// ============================================================================
// Unit Tests
// ============================================================================

test "Lexer single tokens" {
    var lexer = Lexer.init(".");
    var token = lexer.next();
    try std.testing.expectEqual(TokenType.dot, token.type);

    lexer = Lexer.init("(){}[],:");
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.lbrace, lexer.next().type);
    try std.testing.expectEqual(TokenType.rbrace, lexer.next().type);
    try std.testing.expectEqual(TokenType.lbracket, lexer.next().type);
    try std.testing.expectEqual(TokenType.rbracket, lexer.next().type);
    try std.testing.expectEqual(TokenType.comma, lexer.next().type);
    try std.testing.expectEqual(TokenType.colon, lexer.next().type);
}

test "Lexer operators" {
    var lexer = Lexer.init("= != > >= < <= ~");
    try std.testing.expectEqual(TokenType.eq, lexer.next().type);
    try std.testing.expectEqual(TokenType.ne, lexer.next().type);
    try std.testing.expectEqual(TokenType.gt, lexer.next().type);
    try std.testing.expectEqual(TokenType.gte, lexer.next().type);
    try std.testing.expectEqual(TokenType.lt, lexer.next().type);
    try std.testing.expectEqual(TokenType.lte, lexer.next().type);
    try std.testing.expectEqual(TokenType.tilde, lexer.next().type);
}

test "Lexer strings" {
    var lexer = Lexer.init("\"hello\" 'world'");
    var token = lexer.next();
    try std.testing.expectEqual(TokenType.string, token.type);
    try std.testing.expectEqualStrings("hello", token.text);

    token = lexer.next();
    try std.testing.expectEqual(TokenType.string, token.type);
    try std.testing.expectEqualStrings("world", token.text);
}

test "Lexer numbers" {
    var lexer = Lexer.init("42 3.14 -10");
    var token = lexer.next();
    try std.testing.expectEqual(TokenType.number, token.type);
    try std.testing.expectEqualStrings("42", token.text);

    token = lexer.next();
    try std.testing.expectEqual(TokenType.number, token.type);
    try std.testing.expectEqualStrings("3.14", token.text);

    token = lexer.next();
    try std.testing.expectEqual(TokenType.number, token.type);
    try std.testing.expectEqualStrings("-10", token.text);
}

test "Lexer identifiers" {
    var lexer = Lexer.init("orders status_code myField123");
    var token = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, token.type);
    try std.testing.expectEqualStrings("orders", token.text);

    token = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, token.type);
    try std.testing.expectEqualStrings("status_code", token.text);

    token = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, token.type);
    try std.testing.expectEqualStrings("myField123", token.text);
}

test "Lexer keywords" {
    var lexer = Lexer.init("and or not in true false null asc desc count sum avg");
    try std.testing.expectEqual(TokenType.kw_and, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_or, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_not, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_in, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_true, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_false, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_null, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_asc, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_desc, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_count, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_sum, lexer.next().type);
    try std.testing.expectEqual(TokenType.kw_avg, lexer.next().type);
}

test "Lexer complete query" {
    var lexer = Lexer.init("sales.orders.filter(status = \"active\").limit(10)");

    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // sales
    try std.testing.expectEqual(TokenType.dot, lexer.next().type); // .
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // orders
    try std.testing.expectEqual(TokenType.dot, lexer.next().type); // .
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // filter
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type); // (
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // status
    try std.testing.expectEqual(TokenType.eq, lexer.next().type); // =
    try std.testing.expectEqual(TokenType.string, lexer.next().type); // "active"
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type); // )
    try std.testing.expectEqual(TokenType.dot, lexer.next().type); // .
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // limit
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type); // (
    try std.testing.expectEqual(TokenType.number, lexer.next().type); // 10
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type); // )
    try std.testing.expectEqual(TokenType.eof, lexer.next().type);
}

test "Lexer peek does not consume" {
    var lexer = Lexer.init("a b c");
    const peeked = lexer.peek();
    try std.testing.expectEqual(TokenType.identifier, peeked.type);
    try std.testing.expectEqualStrings("a", peeked.text);

    const consumed = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, consumed.type);
    try std.testing.expectEqualStrings("a", consumed.text);
}

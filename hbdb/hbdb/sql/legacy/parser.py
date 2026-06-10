"""
SQL Parser

A simple recursive descent parser for basic SQL.
Supports: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import List, Any, Optional, Dict, Tuple
from enum import Enum, auto


class TokenType(Enum):
    # Keywords
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    CREATE = auto()
    DROP = auto()
    TABLE = auto()
    FROM = auto()
    WHERE = auto()
    INTO = auto()
    VALUES = auto()
    SET = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    NULL = auto()
    PRIMARY = auto()
    KEY = auto()
    INTEGER = auto()
    TEXT = auto()
    REAL = auto()
    BOOLEAN = auto()
    BEGIN = auto()
    COMMIT = auto()
    ROLLBACK = auto()

    # Operators
    EQ = auto()        # =
    NE = auto()        # != or <>
    LT = auto()        # <
    LE = auto()        # <=
    GT = auto()        # >
    GE = auto()        # >=

    # Punctuation
    LPAREN = auto()    # (
    RPAREN = auto()    # )
    COMMA = auto()     # ,
    SEMICOLON = auto() # ;
    STAR = auto()      # *

    # Literals
    STRING = auto()
    NUMBER = auto()
    IDENTIFIER = auto()

    # Special
    EOF = auto()


@dataclass
class Token:
    type: TokenType
    value: Any
    position: int


class Lexer:
    """Tokenize SQL input."""

    KEYWORDS = {
        'SELECT': TokenType.SELECT,
        'INSERT': TokenType.INSERT,
        'UPDATE': TokenType.UPDATE,
        'DELETE': TokenType.DELETE,
        'CREATE': TokenType.CREATE,
        'DROP': TokenType.DROP,
        'TABLE': TokenType.TABLE,
        'FROM': TokenType.FROM,
        'WHERE': TokenType.WHERE,
        'INTO': TokenType.INTO,
        'VALUES': TokenType.VALUES,
        'SET': TokenType.SET,
        'AND': TokenType.AND,
        'OR': TokenType.OR,
        'NOT': TokenType.NOT,
        'NULL': TokenType.NULL,
        'PRIMARY': TokenType.PRIMARY,
        'KEY': TokenType.KEY,
        'INTEGER': TokenType.INTEGER,
        'TEXT': TokenType.TEXT,
        'REAL': TokenType.REAL,
        'BOOLEAN': TokenType.BOOLEAN,
        'BEGIN': TokenType.BEGIN,
        'COMMIT': TokenType.COMMIT,
        'ROLLBACK': TokenType.ROLLBACK,
    }

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.length = len(text)

    def tokenize(self) -> List[Token]:
        tokens = []
        while self.pos < self.length:
            self._skip_whitespace()
            if self.pos >= self.length:
                break

            token = self._next_token()
            if token:
                tokens.append(token)

        tokens.append(Token(TokenType.EOF, None, self.pos))
        return tokens

    def _skip_whitespace(self):
        while self.pos < self.length and self.text[self.pos].isspace():
            self.pos += 1

    def _next_token(self) -> Optional[Token]:
        if self.pos >= self.length:
            return None

        char = self.text[self.pos]
        start = self.pos

        # String literal
        if char in ('"', "'"):
            return self._read_string()

        # Number
        if char.isdigit() or (char == '-' and self.pos + 1 < self.length and self.text[self.pos + 1].isdigit()):
            return self._read_number()

        # Identifier or keyword
        if char.isalpha() or char == '_':
            return self._read_identifier()

        # Operators and punctuation
        if char == '=':
            self.pos += 1
            return Token(TokenType.EQ, '=', start)
        if char == '<':
            self.pos += 1
            if self.pos < self.length and self.text[self.pos] == '=':
                self.pos += 1
                return Token(TokenType.LE, '<=', start)
            if self.pos < self.length and self.text[self.pos] == '>':
                self.pos += 1
                return Token(TokenType.NE, '<>', start)
            return Token(TokenType.LT, '<', start)
        if char == '>':
            self.pos += 1
            if self.pos < self.length and self.text[self.pos] == '=':
                self.pos += 1
                return Token(TokenType.GE, '>=', start)
            return Token(TokenType.GT, '>', start)
        if char == '!' and self.pos + 1 < self.length and self.text[self.pos + 1] == '=':
            self.pos += 2
            return Token(TokenType.NE, '!=', start)
        if char == '(':
            self.pos += 1
            return Token(TokenType.LPAREN, '(', start)
        if char == ')':
            self.pos += 1
            return Token(TokenType.RPAREN, ')', start)
        if char == ',':
            self.pos += 1
            return Token(TokenType.COMMA, ',', start)
        if char == ';':
            self.pos += 1
            return Token(TokenType.SEMICOLON, ';', start)
        if char == '*':
            self.pos += 1
            return Token(TokenType.STAR, '*', start)

        raise SyntaxError(f"Unexpected character: {char} at position {self.pos}")

    def _read_string(self) -> Token:
        start = self.pos
        quote = self.text[self.pos]
        self.pos += 1
        value = ""

        while self.pos < self.length and self.text[self.pos] != quote:
            value += self.text[self.pos]
            self.pos += 1

        if self.pos >= self.length:
            raise SyntaxError(f"Unterminated string at position {start}")

        self.pos += 1  # Skip closing quote
        return Token(TokenType.STRING, value, start)

    def _read_number(self) -> Token:
        start = self.pos
        value = ""

        if self.text[self.pos] == '-':
            value += '-'
            self.pos += 1

        while self.pos < self.length and (self.text[self.pos].isdigit() or self.text[self.pos] == '.'):
            value += self.text[self.pos]
            self.pos += 1

        if '.' in value:
            return Token(TokenType.NUMBER, float(value), start)
        return Token(TokenType.NUMBER, int(value), start)

    def _read_identifier(self) -> Token:
        start = self.pos
        value = ""

        while self.pos < self.length and (self.text[self.pos].isalnum() or self.text[self.pos] == '_'):
            value += self.text[self.pos]
            self.pos += 1

        upper = value.upper()
        if upper in self.KEYWORDS:
            return Token(self.KEYWORDS[upper], upper, start)

        return Token(TokenType.IDENTIFIER, value, start)


# AST Nodes

@dataclass
class SelectStmt:
    columns: List[str]  # ['*'] or ['col1', 'col2']
    table: str
    where: Optional[WhereClause] = None


@dataclass
class InsertStmt:
    table: str
    columns: List[str]
    values: List[Any]


@dataclass
class UpdateStmt:
    table: str
    assignments: Dict[str, Any]
    where: Optional[WhereClause] = None


@dataclass
class DeleteStmt:
    table: str
    where: Optional[WhereClause] = None


@dataclass
class CreateTableStmt:
    table: str
    columns: List[Tuple[str, str, bool]]  # (name, type, is_pk)


@dataclass
class DropTableStmt:
    table: str


@dataclass
class BeginStmt:
    pass


@dataclass
class CommitStmt:
    pass


@dataclass
class RollbackStmt:
    pass


@dataclass
class WhereClause:
    column: str
    operator: str
    value: Any


Statement = SelectStmt | InsertStmt | UpdateStmt | DeleteStmt | CreateTableStmt | DropTableStmt | BeginStmt | CommitStmt | RollbackStmt


class SQLParser:
    """Parse SQL statements into AST."""

    def __init__(self):
        self.tokens: List[Token] = []
        self.pos = 0

    def parse(self, sql: str) -> Statement:
        """Parse a SQL statement."""
        lexer = Lexer(sql)
        self.tokens = lexer.tokenize()
        self.pos = 0

        return self._parse_statement()

    def _current(self) -> Token:
        return self.tokens[self.pos]

    def _peek(self, offset: int = 0) -> Token:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return self.tokens[-1]

    def _advance(self) -> Token:
        token = self._current()
        self.pos += 1
        return token

    def _expect(self, token_type: TokenType) -> Token:
        token = self._current()
        if token.type != token_type:
            raise SyntaxError(f"Expected {token_type}, got {token.type} at position {token.position}")
        return self._advance()

    def _match(self, *token_types: TokenType) -> bool:
        return self._current().type in token_types

    def _parse_statement(self) -> Statement:
        token = self._current()

        if token.type == TokenType.SELECT:
            return self._parse_select()
        elif token.type == TokenType.INSERT:
            return self._parse_insert()
        elif token.type == TokenType.UPDATE:
            return self._parse_update()
        elif token.type == TokenType.DELETE:
            return self._parse_delete()
        elif token.type == TokenType.CREATE:
            return self._parse_create()
        elif token.type == TokenType.DROP:
            return self._parse_drop()
        elif token.type == TokenType.BEGIN:
            self._advance()
            return BeginStmt()
        elif token.type == TokenType.COMMIT:
            self._advance()
            return CommitStmt()
        elif token.type == TokenType.ROLLBACK:
            self._advance()
            return RollbackStmt()
        else:
            raise SyntaxError(f"Unexpected token: {token.type}")

    def _parse_select(self) -> SelectStmt:
        self._expect(TokenType.SELECT)

        # Columns
        columns = []
        if self._match(TokenType.STAR):
            self._advance()
            columns = ['*']
        else:
            columns.append(self._expect(TokenType.IDENTIFIER).value)
            while self._match(TokenType.COMMA):
                self._advance()
                columns.append(self._expect(TokenType.IDENTIFIER).value)

        self._expect(TokenType.FROM)
        table = self._expect(TokenType.IDENTIFIER).value

        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_where()

        return SelectStmt(columns=columns, table=table, where=where)

    def _parse_insert(self) -> InsertStmt:
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)
        table = self._expect(TokenType.IDENTIFIER).value

        # Optional column list
        columns = []
        if self._match(TokenType.LPAREN):
            self._advance()
            columns.append(self._expect(TokenType.IDENTIFIER).value)
            while self._match(TokenType.COMMA):
                self._advance()
                columns.append(self._expect(TokenType.IDENTIFIER).value)
            self._expect(TokenType.RPAREN)

        self._expect(TokenType.VALUES)
        self._expect(TokenType.LPAREN)

        values = [self._parse_value()]
        while self._match(TokenType.COMMA):
            self._advance()
            values.append(self._parse_value())

        self._expect(TokenType.RPAREN)

        return InsertStmt(table=table, columns=columns, values=values)

    def _parse_update(self) -> UpdateStmt:
        self._expect(TokenType.UPDATE)
        table = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.SET)

        assignments = {}
        col = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.EQ)
        val = self._parse_value()
        assignments[col] = val

        while self._match(TokenType.COMMA):
            self._advance()
            col = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.EQ)
            val = self._parse_value()
            assignments[col] = val

        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_where()

        return UpdateStmt(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteStmt:
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)
        table = self._expect(TokenType.IDENTIFIER).value

        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_where()

        return DeleteStmt(table=table, where=where)

    def _parse_create(self) -> CreateTableStmt:
        self._expect(TokenType.CREATE)
        self._expect(TokenType.TABLE)
        table = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.LPAREN)

        columns = []
        col_name = self._expect(TokenType.IDENTIFIER).value
        col_type = self._advance().value  # Type keyword
        is_pk = False
        if self._match(TokenType.PRIMARY):
            self._advance()
            self._expect(TokenType.KEY)
            is_pk = True
        columns.append((col_name, col_type, is_pk))

        while self._match(TokenType.COMMA):
            self._advance()
            col_name = self._expect(TokenType.IDENTIFIER).value
            col_type = self._advance().value
            is_pk = False
            if self._match(TokenType.PRIMARY):
                self._advance()
                self._expect(TokenType.KEY)
                is_pk = True
            columns.append((col_name, col_type, is_pk))

        self._expect(TokenType.RPAREN)

        return CreateTableStmt(table=table, columns=columns)

    def _parse_drop(self) -> DropTableStmt:
        self._expect(TokenType.DROP)
        self._expect(TokenType.TABLE)
        table = self._expect(TokenType.IDENTIFIER).value
        return DropTableStmt(table=table)

    def _parse_where(self) -> WhereClause:
        self._expect(TokenType.WHERE)
        column = self._expect(TokenType.IDENTIFIER).value

        op_token = self._advance()
        op_map = {
            TokenType.EQ: '=',
            TokenType.NE: '!=',
            TokenType.LT: '<',
            TokenType.LE: '<=',
            TokenType.GT: '>',
            TokenType.GE: '>=',
        }
        operator = op_map.get(op_token.type, '=')

        value = self._parse_value()

        return WhereClause(column=column, operator=operator, value=value)

    def _parse_value(self) -> Any:
        token = self._current()
        if token.type == TokenType.STRING:
            self._advance()
            return token.value
        elif token.type == TokenType.NUMBER:
            self._advance()
            return token.value
        elif token.type == TokenType.NULL:
            self._advance()
            return None
        elif token.type == TokenType.IDENTIFIER:
            self._advance()
            return token.value
        else:
            raise SyntaxError(f"Expected value, got {token.type}")

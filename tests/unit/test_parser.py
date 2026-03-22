"""Unit tests for src/utils/parser.py"""
import pytest
from src.utils.parser import parse_json, parse_csv, parse_input, ParseError


def test_parse_json_array():
    content = '[{"a": 1}, {"a": 2}]'
    result = parse_json(content)
    assert result == [{"a": 1}, {"a": 2}]


def test_parse_json_single_object():
    content = '{"a": 1}'
    result = parse_json(content)
    assert result == [{"a": 1}]


def test_parse_json_newline_delimited():
    content = '{"a": 1}\n{"a": 2}'
    result = parse_json(content)
    assert result == [{"a": 1}, {"a": 2}]


def test_parse_json_empty_raises():
    with pytest.raises(ParseError):
        parse_json("")


def test_parse_json_whitespace_only_raises():
    with pytest.raises(ParseError):
        parse_json("   \n  ")


def test_parse_json_empty_array_raises():
    with pytest.raises(ParseError):
        parse_json("[]")


def test_parse_json_malformed_raises():
    with pytest.raises(ParseError):
        parse_json("{bad json")


def test_parse_csv_basic():
    content = "a,b\n1,2\n3,4"
    result = parse_csv(content)
    assert result == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_parse_csv_empty_raises():
    with pytest.raises(ParseError):
        parse_csv("")


def test_parse_csv_header_only_raises():
    with pytest.raises(ParseError):
        parse_csv("a,b,c")


def test_parse_input_dispatches_json():
    content = '[{"x": 1}]'
    assert parse_input(content, "json") == [{"x": 1}]


def test_parse_input_dispatches_csv():
    content = "x\n1"
    assert parse_input(content, "csv") == [{"x": "1"}]


def test_parse_input_unknown_format_raises():
    with pytest.raises(ParseError):
        parse_input("data", "xml")

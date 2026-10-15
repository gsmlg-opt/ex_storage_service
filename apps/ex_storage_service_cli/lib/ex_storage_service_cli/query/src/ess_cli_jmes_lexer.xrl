% MIT-derived code; full copyright and license in ../NOTICE.txt.
Definitions.

Whitespace = [\s\t\n\r]
Unquoted = [a-zA-Z_][0-9a-zA-Z_]*
Quoted = "([^"\\]*(\\.[^"\\]*)*)"
Raw = '([^'\\]*(\\.[^'\\]*)*)'
JSON = `([^`\\]*(\\.[^`\\]*)*)`
Integer = -?[0-9]+
LeftParen = \(
RightParen = \)
LeftBracket = \[
RightBracket = \]
BracketQuery = \[\?
LeftCurly = {
RightCurly = }
Wildcard = \*
At = @
Pipe = \|
Or = \|\|
And = &&
Eq = ==
Neq = !=
Lt = <
Gt = >
Lte = <=
Gte = >=
Not = !
Dot = \.
Colon = :
Comma = ,
Ampersand = &

Rules.

{Whitespace} : skip_token.
{Unquoted} : {token, {unquoted, TokenLine, list_to_binary(TokenChars)}}.
{Quoted} : handle_quoted(TokenLine,TokenChars,TokenLen).
{Raw} : {token, {raw, TokenLine, unescape_raw(TokenChars,TokenLen)}}.
{JSON} : {token, {json, TokenLine, unescape_json(TokenChars,TokenLen)}}.
{Integer} : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{LeftParen} : {token, {'(', TokenLine, TokenChars}}.
{RightParen} : {token, {')', TokenLine, TokenChars}}.
{LeftBracket} : {token, {'[', TokenLine, TokenChars}}.
{RightBracket} : {token, {']', TokenLine, TokenChars}}.
{BracketQuery} : {token, {'[?', TokenLine, TokenChars}}.
{LeftCurly} : {token, {'{', TokenLine, TokenChars}}.
{RightCurly} : {token, {'}', TokenLine, TokenChars}}.
{Wildcard} : {token, {'*', TokenLine, TokenChars}}.
{At} : {token, {'@', TokenLine, TokenChars}}.
{Pipe} : {token, {'|', TokenLine, TokenChars}}.
{Or} : {token, {'||', TokenLine, TokenChars}}.
{And} : {token, {'&&', TokenLine, TokenChars}}.
{Eq} : {token, {'==', TokenLine, TokenChars}}.
{Neq} : {token, {'!=', TokenLine, TokenChars}}.
{Lt} : {token, {'<', TokenLine, TokenChars}}.
{Gt} : {token, {'>', TokenLine, TokenChars}}.
{Lte} : {token, {'<=', TokenLine, TokenChars}}.
{Gte} : {token, {'>=', TokenLine, TokenChars}}.
{Not} : {token, {'!', TokenLine, TokenChars}}.
{Dot} : {token, {'.', TokenLine, TokenChars}}.
{Colon} : {token, {':', TokenLine, TokenChars}}.
{Comma} : {token, {',', TokenLine, TokenChars}}.
{Ampersand} : {token, {'&', TokenLine, TokenChars}}.

Erlang code.

handle_quoted(TokenLine,TokenChars,TokenLen) ->
    try
        {token, {quoted, TokenLine, unescape_quoted(TokenChars,TokenLen)}}
    catch
        throw:Message -> {error, Message}
    end.

unescape_quoted(TokenChars,_TokenLen) ->
    case 'Elixir.JSON':decode(unicode:characters_to_binary(TokenChars)) of
        {ok, Value} -> Value;
        {error, _} -> throw("invalid quoted identifier")
    end.

unescape_raw(TokenChars,TokenLen) ->
    unescape_squote(unicode:characters_to_binary(strip(TokenChars,TokenLen))).

unescape_json(TokenChars,TokenLen) ->
    unescape_bquote(unicode:characters_to_binary(strip(TokenChars,TokenLen))).

strip(TokenChars,TokenLen) ->
    lists:sublist(TokenChars,2,TokenLen-2).

unescape_squote(<<$\\,$',T/binary>>) ->
    U = unescape_squote(T),
    <<$',U/binary>>;
unescape_squote(<<H/utf8,T/binary>>) ->
    U = unescape_squote(T),
    <<H/utf8,U/binary>>;
unescape_squote(<<>>) -> <<>>.

unescape_bquote(<<$\\,$`,T/binary>>) ->
    U = unescape_bquote(T),
    <<$`,U/binary>>;
unescape_bquote(<<H/utf8,T/binary>>) ->
    U = unescape_bquote(T),
    <<H/utf8,U/binary>>;
unescape_bquote(<<>>) -> <<>>.


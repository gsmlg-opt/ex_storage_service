%-
% Copyright (c) 2012-2014 Yakaz
% Copyright (c) 2016-2022 Jean-Sébastien Pédron <jean-sebastien.pedron@dumbbell.fr>
% All rights reserved.
%
% Redistribution and use in source and binary forms, with or without
% modification, are permitted provided that the following conditions
% are met:
% 1. Redistributions of source code must retain the above copyright
%    notice, this list of conditions and the following disclaimer.
% 2. Redistributions in binary form must reproduce the above copyright
%    notice, this list of conditions and the following disclaimer in the
%    documentation and/or other materials provided with the distribution.
%
% THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
% ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
% ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
% OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
% HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
% LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
% OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
% SUCH DAMAGE.

-ifndef(internal_ess_cli_yaml_parser_hrl).
-define(internal_ess_cli_yaml_parser_hrl, true).

-include("ess_cli_yaml_tokens.hrl").

%% -------------------------------------------------------------------
%% Options.
%% -------------------------------------------------------------------

-type ess_cli_yaml_parser_token_fun() ::
        fun((ess_cli_yaml_token() | term()) ->
              ok | {ok, ess_cli_yaml_parser_token_fun()} | term()).

-type ess_cli_yaml_parser_option() :: {default_tags, [{tag_uri(), tag_prefix()}]}
                              | {document_version, document_version()}
                              | {io_blocksize, pos_integer()}
                              | {token_fun,
                                 ess_cli_yaml_parser_token_fun() | acc | drop}.

%% -------------------------------------------------------------------
%% Main record to store the scanner state.
%% -------------------------------------------------------------------

-record(impl_key, {
    possible  = false :: boolean(),
    required  = false :: boolean(),
    line              :: position() | undefined,
    col               :: position() | undefined,
    chars_idx = 1     :: pos_integer(),
    token_idx = 1     :: pos_integer()
  }).

-record(bcoll, {
    kind   = root     :: root | sequence | mapping,
    indent = 0        :: 0    | position(),
    kidx   = -1       :: pos_integer() | -1, %% Last key index.
    kline  = 1        :: position(),         %% Last key line.
    kcol   = 1        :: position(),         %% Last key column.
    vidx   = -1       :: pos_integer() | -1, %% Last value index.
    vline  = 1        :: position(),         %% Last value line.
    vcol   = 1        :: position()          %% Last value column.
  }).

-record(fcoll, {
    kind   = sequence :: sequence | mapping | single_mapping,
    kidx   = -1       :: pos_integer() | -1, %% Last key/entry index.
    kline  = 1        :: position(),         %% Last key/entry line.
    kcol   = 1        :: position(),         %% Last key/entry column.
    vidx   = -1       :: pos_integer() | -1, %% Last value index.
    vline  = 1        :: position(),         %% Last value line.
    vcol   = 1        :: position()          %% Last value column.
  }).

-type stream_state_fun() ::
      fun((unicode_string(), position(), position(), non_neg_integer(),
           ess_cli_yaml_parser()) ->
             ess_cli_yaml_parser()
           | {continue, ess_cli_yaml_parser()}
           | no_return()).

-record(ess_cli_yaml_parser, {
    %%
    %% Buffer management.
    %%

    %% An indication of the source of the stream, eg. a file.
    source       :: any(),
    options = [] :: [ess_cli_yaml_parser_option()],

    %% Raw data corresponds to Unicode characters not decoded yet. The
    %% raw index indicates where the raw data is in the stream; it
    %% equals the amount of data already decoded in bytes. The raw_eos
    %% is a flag indicating the end of stream; this flag is set when the
    %% last chunk is seen.
    raw_data = <<>>  :: binary(),
    raw_idx  = 0     :: non_neg_integer(),
    raw_eos  = false :: boolean(),

    %% Characters data are the decoded Unicode characters but not
    %% scanned yet. The length corresponds to the number of characters
    %% in this list. The index indicates where the characters data is in
    %% the stream; it equals to the amount of data already scanned in
    %% characters.
    chars     = "" :: unicode_string(),
    chars_len = 0  :: non_neg_integer(),
    chars_idx = 0  :: non_neg_integer(),

    %% Cursor "position". While scanning the stream, we keep the line
    %% and column number. We also need to remember the last token end
    %% line and column.
    line                = 1     :: pos_integer(),
    col                 = 1     :: pos_integer(),
    endpos_set_by_token = false :: boolean(),
    last_token_endline  = 1     :: pos_integer(),
    last_token_endcol   = 1     :: pos_integer(),

    %%
    %% Stream information.
    %%

    %% Character encoding of the stream. It must be a valid Unicode
    %% character encoding and it must not change after stream start.
    encoding :: encoding() | undefined,

    %%
    %% Document information.
    %% Those information are reset between each document.
    %%

    %% "doc_started" indicates if the document is started or not.
    %% This is used to know when directives are allowed for example.
    %% The document version is set by a YAML directive or to
    %% ?IMPLICIT_YAML_VERSION when a document starts if there were no
    %% directive.
    doc_started = false :: boolean(),
    doc_version         :: document_version() | undefined,

    %% "tags" is a dictionary containing default tag handles and those
    %% defined by TAG directives. It's used during tag resolution. The
    %% last tag property is stored in "last_tag" and is attached to a
    %% node only when this node is about to be emitted.
    tags = dict:new() :: tags_table(),

    %%
    %% Parsing state.
    %%

    %% The stream state corresponds to the name of the function to call
    %% when more data is available. This state is influenced by several
    %% parameters:
    %%   o  the block-context indentation prefixes (a stack);
    %%   o  the level of flow-context nesting (a stack too).
    stream_state = fun start_stream/5 :: stream_state_fun(),
    parent_colls = []                 :: [#bcoll{} | #fcoll{}],
    cur_coll     = #bcoll{}           :: #bcoll{} | #fcoll{},

    %% When parsing a flow collection, we keep a "pending_entry" flag.
    %% The next token may trigger the queueing of a sequence entry token.
    pending_entry = false :: boolean(),

    %% We also keep a flag to know if the next expected token is a key:
    %% value pair.
    waiting_for_kvpair = false :: boolean(),

    %% Implicit keys handling.
    %% We need to know if the next token could be an implicit key.
    %% Furthermore, while searching for ':', marking the "end" of an
    %% implicit key, we need to store the positions where an implicit
    %% key could appear. In block context, an implicit key can't contain
    %% an implicit key. But in flow context, an implicit key can embed
    %% another implicit key.
    ik_allowed = false :: boolean(),
    ik_stack   = []    :: [#impl_key{}],

    %% We remember if the last queued token is JSON-like. JSON-like
    %% nodes are single- and double-quoted scalars and flow collections.
    %% Therefore, the JSON-like tokens are single- and double-quoted
    %% scalar tokens and flow collection end tokens.
    last_is_json_like = false :: boolean(),

    %% Did the last parsing eat the newline?
    missed_nl = false :: boolean(),

    %%
    %% Parsing output.
    %%

    %% Callbacks.
    token_fun = acc :: ess_cli_yaml_parser_token_fun() | acc | drop,

    %% List of scanned tokens with counters.
    tokens        = []  :: [ess_cli_yaml_token()],
    tks_queued    = 0   :: non_neg_integer(),
    tks_first_idx = 1   :: pos_integer(),
    tks_emitted   = 0   :: non_neg_integer(),

    %% We keep a copy of the last emitted token. This is used for
    %% verification purpose. For instance, two scalars in a row is an
    %% error.
    last_tag            :: ess_cli_yaml_tag()    | undefined,
    last_anchor         :: ess_cli_yaml_anchor() | undefined,
    last_token          :: ess_cli_yaml_token()  | undefined,

    %% List of warnings and errors. The scan won't necessarily stop at
    %% the first error.
    has_errors = false  :: boolean(),
    errors     = []     :: [#ess_cli_yaml_parsing_error{}],

    %% When the user doesn't specify a "token_fun" callback module all
    %% tokens to be emitted are stored in the following list.
    tks_ready  = []     :: [ess_cli_yaml_token()]
  }).

-endif.

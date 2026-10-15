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

-ifndef(ess_cli_yaml_nodes_hrl).
-define(ess_cli_yaml_nodes_hrl, true).

-include("ess_cli_yaml_types.hrl").

%% CAUTION:
%% Records defined in this file have default values for all members.
%% Those default values are often bad values but this is needed so that
%% Erlang won't add "undefined" in our back to the allowed values in the
%% type specifications.

%% -------------------------------------------------------------------
%% Nodes specifications.
%% -------------------------------------------------------------------

%% String (Failsafe Schema).
-record(ess_cli_yaml_str, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    text   = ""                   :: unicode_string()
  }).
-type ess_cli_yaml_str()                :: #ess_cli_yaml_str{}.
-type ess_cli_yaml_simple_str()         :: unicode_string().

%% Null (Core Schema).
-record(ess_cli_yaml_null, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list()
  }).
-type ess_cli_yaml_null()               :: #ess_cli_yaml_null{}.
-type ess_cli_yaml_simple_null()        :: null.

%% Boolean (Core Schema).
-record(ess_cli_yaml_bool, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    value  = true                 :: boolean()
  }).
-type ess_cli_yaml_bool()               :: #ess_cli_yaml_bool{}.
-type ess_cli_yaml_simple_bool()        :: boolean().

%% Integer (Core Schema).
-record(ess_cli_yaml_int, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    value  = 0                    :: integer()
  }).
-type ess_cli_yaml_int()                :: #ess_cli_yaml_int{}.
-type ess_cli_yaml_simple_int()         :: integer().

%% Float (Core Schema).
-record(ess_cli_yaml_float, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    value  = 0.0                  :: float() | '+inf' | '-inf' | 'nan'
  }).
-type ess_cli_yaml_float()              :: #ess_cli_yaml_float{}.
-type ess_cli_yaml_simple_float()       :: float().

%% Binary (Core Schema).
-record(ess_cli_yaml_binary, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    data   = <<"">>               :: bitstring()
  }).
-type ess_cli_yaml_binary()                :: #ess_cli_yaml_binary{}.
-type ess_cli_yaml_simple_binary()         :: bitstring().

%% Erlang atom.
-record(ess_cli_yaml_erlang_atom, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    name                          :: atom()
  }).
-type ess_cli_yaml_erlang_atom()        :: #ess_cli_yaml_erlang_atom{}.
-type ess_cli_yaml_simple_erlang_atom() :: atom().

%% Erlang anonymous function.
-record(ess_cli_yaml_erlang_fun, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    function                      :: function(),
    text                          :: unicode_string()
  }).
-type ess_cli_yaml_erlang_fun()         :: #ess_cli_yaml_erlang_fun{}.
-type ess_cli_yaml_simple_erlang_fun()  :: function().

%% Timestamp.
-type year()   :: non_neg_integer(). %% Types from stdlib/src/calendar.erl.
-type month()  :: 1..12.
-type day()    :: 1..31.
-type hour()   :: 0..23.
-type minute() :: 0..59.
-type second() :: 0..59.
-record(ess_cli_yaml_timestamp, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    year                          :: year() | undefined,
    month                         :: month() | undefined,
    day                           :: day() | undefined,
    hour   = 0                    :: hour(),
    minute = 0                    :: minute(),
    second = 0                    :: second(),
    frac   = 0                    :: non_neg_integer(),
    tz     = 0                    :: integer()
  }).
-type ess_cli_yaml_timestamp()          :: #ess_cli_yaml_timestamp{}.
-type ess_cli_yaml_simple_timestamp()   :: calendar:datetime()
                                   | {undefined, calendar:time()}.

%% Sequence (Failsafe Schema).
-record(ess_cli_yaml_seq, {
    module  = undefined           :: atom(),
    tag     = "!"                 :: tag_uri(),
    pres    = []                  :: list(),
    entries = []                  :: [ess_cli_yaml_node()],
    count   = 0                   :: non_neg_integer()
  }).
-type ess_cli_yaml_seq()                :: #ess_cli_yaml_seq{}.
-type ess_cli_yaml_simple_seq()         :: [ess_cli_yaml_simple_node()].
-type ess_cli_yaml_partial_seq()        :: {
                                       atom(),
                                       {seq, non_neg_integer()},
                                       [
                                         ess_cli_yaml_node()
                                         | ess_cli_yaml_simple_node()
                                         | '$insert_here'
                                       ]
                                     }.

%% Mapping (Failsafe Schema).
-record(ess_cli_yaml_map, {
    module = undefined            :: atom(),
    tag    = "!"                  :: tag_uri(),
    pres   = []                   :: list(),
    pairs  = []                   :: [{ess_cli_yaml_node(), ess_cli_yaml_node()}]
  }).
-type ess_cli_yaml_map()                :: #ess_cli_yaml_map{}.
-type ess_cli_yaml_simple_map()         :: [{
                                       ess_cli_yaml_simple_node(),
                                       ess_cli_yaml_simple_node()
                                     }].
-type ess_cli_yaml_partial_map()        :: {
                                       atom(),
                                       {map,
                                         ess_cli_yaml_node()
                                         | ess_cli_yaml_simple_node()
                                         | undefined},
                                       [{
                                         ess_cli_yaml_node()
                                         | ess_cli_yaml_simple_node()
                                         | '$insert_here',
                                         ess_cli_yaml_node()
                                         | ess_cli_yaml_simple_node()
                                         | '$insert_here' | undefined
                                       }]
                                     }.

%% Document.
-record(ess_cli_yaml_doc, {
    root = undefined              :: ess_cli_yaml_node()
                                   | ess_cli_yaml_simple_node() | undefined
  }).
-type ess_cli_yaml_doc()                :: #ess_cli_yaml_doc{root :: ess_cli_yaml_node()}.
-type ess_cli_yaml_simple_doc()         :: ess_cli_yaml_simple_node().
-type ess_cli_yaml_partial_doc()        :: #ess_cli_yaml_doc{}.

%% -------------------------------------------------------------------
%% Final data type specifications.
%% -------------------------------------------------------------------

-type ess_cli_yaml_user_node()          :: tuple().
-type ess_cli_yaml_user_simple_node()   :: term().

-type ess_cli_yaml_node()               :: ess_cli_yaml_seq()
                                   | ess_cli_yaml_map()
                                   | ess_cli_yaml_str()
                                   | ess_cli_yaml_null()
                                   | ess_cli_yaml_bool()
                                   | ess_cli_yaml_int()
                                   | ess_cli_yaml_float()
                                   | ess_cli_yaml_timestamp()
                                   | ess_cli_yaml_erlang_atom()
                                   | ess_cli_yaml_erlang_fun()
                                   | ess_cli_yaml_user_node().

-type ess_cli_yaml_simple_node()        :: ess_cli_yaml_simple_seq()
                                   | ess_cli_yaml_simple_map()
                                   | ess_cli_yaml_simple_str()
                                   | ess_cli_yaml_simple_null()
                                   | ess_cli_yaml_simple_bool()
                                   | ess_cli_yaml_simple_int()
                                   | ess_cli_yaml_simple_float()
                                   | ess_cli_yaml_simple_timestamp()
                                   | ess_cli_yaml_simple_erlang_atom()
                                   | ess_cli_yaml_simple_erlang_fun()
                                   | ess_cli_yaml_user_simple_node().

-type ess_cli_yaml_partial_node()       :: ess_cli_yaml_partial_seq()
                                   | ess_cli_yaml_partial_map()
                                   | ess_cli_yaml_str()
                                   | ess_cli_yaml_null()
                                   | ess_cli_yaml_bool()
                                   | ess_cli_yaml_int()
                                   | ess_cli_yaml_float()
                                   | ess_cli_yaml_timestamp()
                                   | ess_cli_yaml_erlang_atom()
                                   | ess_cli_yaml_erlang_fun()
                                   | ess_cli_yaml_user_node()
                                   | ess_cli_yaml_simple_str()
                                   | ess_cli_yaml_simple_null()
                                   | ess_cli_yaml_simple_bool()
                                   | ess_cli_yaml_simple_int()
                                   | ess_cli_yaml_simple_float()
                                   | ess_cli_yaml_simple_timestamp()
                                   | ess_cli_yaml_simple_erlang_atom()
                                   | ess_cli_yaml_simple_erlang_fun()
                                   | ess_cli_yaml_user_simple_node().

%% -------------------------------------------------------------------
%% Macros to access common members of the node records.
%% -------------------------------------------------------------------

-define(NODE_MOD(N),  element(#ess_cli_yaml_str.module, N)).
-define(NODE_TAG(N),  element(#ess_cli_yaml_str.tag, N)).
-define(NODE_PRES(N), element(#ess_cli_yaml_str.pres, N)).

%% -------------------------------------------------------------------
%% List of modules implementing the Core Schema nodes.
%% -------------------------------------------------------------------

-define(FAILSAFE_SCHEMA_MODS, [
    ess_cli_yaml_node_str,
    ess_cli_yaml_node_seq,
    ess_cli_yaml_node_map
  ]).

-define(JSON_SCHEMA_MODS, [
    ess_cli_yaml_node_null_json,
    ess_cli_yaml_node_bool_json,
    ess_cli_yaml_node_int_json,
    ess_cli_yaml_node_float_json,
    ess_cli_yaml_node_str_json,
    ess_cli_yaml_node_seq,
    ess_cli_yaml_node_map
  ]).

-define(CORE_SCHEMA_MODS, [
    ess_cli_yaml_node_null,
    ess_cli_yaml_node_bool,
    ess_cli_yaml_node_int,
    ess_cli_yaml_node_float,
    ess_cli_yaml_node_str,
    ess_cli_yaml_node_binary,
    ess_cli_yaml_node_seq,
    ess_cli_yaml_node_map
  ]).

-define(YAML11_SCHEMA_MODS, [
    ess_cli_yaml_node_null,
    ess_cli_yaml_node_bool_ext,
    ess_cli_yaml_node_int_ext,
    ess_cli_yaml_node_float_ext,
    ess_cli_yaml_node_str,
    ess_cli_yaml_node_binary,
    ess_cli_yaml_node_seq,
    ess_cli_yaml_node_map
  ]).

-endif.

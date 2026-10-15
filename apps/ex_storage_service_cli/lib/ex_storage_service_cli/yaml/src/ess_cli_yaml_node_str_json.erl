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

%% @private

-module(ess_cli_yaml_node_str_json).

-include("ess_cli_yaml_errors.hrl").
-include("ess_cli_yaml_tokens.hrl").
-include("ess_cli_yaml_nodes.hrl").
-include("internal/ess_cli_yaml_constr.hrl").

%% Public API.
-export([
    tags/0,
    try_construct_token/3,
    construct_token/3,
    node_pres/1
  ]).

-define(TAG, "tag:yaml.org,2002:str").

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

tags() -> [?TAG].

try_construct_token(Constr, Node, #ess_cli_yaml_scalar{} = Token) ->
    construct_token(Constr, Node, Token);
try_construct_token(_, _, _) ->
    unrecognized.

construct_token(#ess_cli_yaml_constr{detailed_constr = false, ext_options = Options},
  undefined, #ess_cli_yaml_scalar{text = Text, tag = #ess_cli_yaml_tag{uri = Uri}})
when Uri /= {non_specific, "?"} ->
    Node = case proplists:get_value(str_node_as_binary, Options, false) of
        false    -> Text;
        true     -> unicode:characters_to_binary(Text);
        Encoding -> unicode:characters_to_binary(Text, unicode, Encoding)
    end,
    {finished, Node};
construct_token(#ess_cli_yaml_constr{detailed_constr = true, ext_options = Options},
  undefined, #ess_cli_yaml_scalar{text = Text, tag = #ess_cli_yaml_tag{uri = Uri}} = Token)
when Uri /= {non_specific, "?"} ->
    Text1 = case proplists:get_value(str_node_as_binary, Options, false) of
        false    -> Text;
        true     -> unicode:characters_to_binary(Text);
        Encoding -> unicode:characters_to_binary(Text, unicode, Encoding)
    end,
    Pres = ess_cli_yaml_constr:get_pres_details(Token),
    Node = #ess_cli_yaml_str{
      module = ?MODULE,
      tag    = ?TAG,
      pres   = Pres,
      text   = Text1
    },
    {finished, Node};

construct_token(_, _, Token) ->
    Error = #ess_cli_yaml_parsing_error{
      name   = not_a_string,
      token  = Token,
      text   = "Invalid string",
      line   = ?TOKEN_LINE(Token),
      column = ?TOKEN_COLUMN(Token)
    },
    throw(Error).

node_pres(Node) ->
    ?NODE_PRES(Node).

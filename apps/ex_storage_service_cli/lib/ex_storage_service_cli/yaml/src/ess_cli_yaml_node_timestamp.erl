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

-module(ess_cli_yaml_node_timestamp).
-include("ess_cli_yaml_tokens.hrl").
-include("ess_cli_yaml_errors.hrl").
-include("ess_cli_yaml_nodes.hrl").
-export([tags/0, try_construct_token/3, construct_token/3, node_pres/1]).
tags() -> ["tag:yaml.org,2002:timestamp"].
try_construct_token(Constr, Node,
  #ess_cli_yaml_scalar{tag = #ess_cli_yaml_tag{uri = {non_specific, "?"}}} = Token) ->
    try construct_token(Constr, Node, Token) catch _:_ -> unrecognized end;
try_construct_token(_, _, _) -> unrecognized.
construct_token(_, undefined, #ess_cli_yaml_scalar{text = Text}) ->
    case 'Elixir.ExStorageServiceCli.YAML.Scalar':timestamp(unicode:characters_to_binary(Text)) of
        {ok, Value} -> {finished, Value};
        error -> throw(ess_cli_yaml_invalid_timestamp)
    end.
node_pres(_) -> undefined.

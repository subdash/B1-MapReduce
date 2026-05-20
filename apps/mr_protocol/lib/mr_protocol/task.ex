defmodule MrProtocol.Task do
  @callback map(key :: String.t(), value :: String.t()) :: [{String.t(), term()}]
  @callback reduce(key :: String.t(), values :: [term()]) :: {String.t(), term()}
  @callback combine(key :: String.t(), values :: [term()]) :: {String.t(), term()}

  @optional_callbacks combine: 2
end

defmodule ExStorageServiceWeb.Layouts do
  @moduledoc """
  This module holds different layouts used by your application.

  The layouts are defined in the templates directory:
    lib/ex_storage_service_web/components/layouts/root.html.heex
    lib/ex_storage_service_web/components/layouts/app.html.heex
  """
  use ExStorageServiceWeb, :html

  embed_templates "layouts/*"
end

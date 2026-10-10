defmodule ExStorageServiceWeb.LayoutsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias ExStorageServiceWeb.Layouts

  @info %{
    version: "0.6.6",
    environment: :prod,
    git_ref: "v0.6.6",
    git_sha: "0123456789abcdef0123456789abcdef01234567",
    build_time: "2026-10-10T01:00:00Z"
  }

  test "adds the dev suffix only for development builds" do
    html = render_component(&Layouts.version_badge/1, info: %{@info | environment: :dev})
    assert html =~ "v0.6.6-dev"
    assert html =~ "Environment: dev"

    for environment <- [:prod, :test] do
      html = render_component(&Layouts.version_badge/1, info: %{@info | environment: environment})
      assert html =~ "v0.6.6"
      refute html =~ "v0.6.6-dev"
    end
  end

  test "exposes build details through a keyboard-accessible tooltip" do
    html = render_component(&Layouts.version_badge/1, info: @info)

    assert html =~ ~s(<button)
    assert html =~ ~s(aria-describedby="app-version-tooltip")
    assert html =~ ~s(interestfor="app-version-tooltip")
    assert html =~ ~s(role="tooltip")
    assert html =~ ~s(popover="hint")
    assert html =~ "Version: 0.6.6"
    assert html =~ "Git ref: v0.6.6"
    assert html =~ "Commit: #{@info.git_sha}"
    assert html =~ "Build time: #{@info.build_time}"
  end

  test "labels unavailable Git metadata explicitly" do
    html =
      render_component(&Layouts.version_badge/1,
        info: %{@info | git_ref: nil, git_sha: nil}
      )

    assert html =~ "Git ref: Unknown"
    assert html =~ "Commit: Unknown"
  end

  test "renders the version next to the appbar brand" do
    html =
      render_component(&Layouts.app/1,
        flash: %{},
        inner_content: "Page content",
        page_title: "Dashboard"
      )

    document = LazyHTML.from_document(html)
    assert LazyHTML.query(document, ".appbar #app-version-trigger") |> LazyHTML.text() =~ "v"

    assert LazyHTML.query(document, ".appbar #app-version-tooltip") |> LazyHTML.text() =~
             "Git ref:"

    assert LazyHTML.query(document, ".appbar-brand #app-version-trigger") |> Enum.empty?()
  end
end

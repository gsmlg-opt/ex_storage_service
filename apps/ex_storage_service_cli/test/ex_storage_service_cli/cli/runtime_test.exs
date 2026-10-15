Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.RuntimeTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.Runtime
  alias ExStorageServiceCli.{Context, Test.WireServer, Target}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  test "invocation stops its task/provider tree", %{
    tmp_dir: root
  } do
    parent = self()

    assert :done =
             Runtime.with_invocation(fn runtime ->
               assert is_pid(Runtime.task_supervisor(runtime))

               assert {:ok, context} =
                        Runtime.aws_context(runtime, %{},
                          env: %{"AWS_ACCESS_KEY_ID" => "a", "AWS_SECRET_ACCESS_KEY" => "s"},
                          home: root
                        )

               assert is_pid(context.credential_owner)

               send(
                 parent,
                 {:owned, runtime.supervisor, runtime.tasks, runtime.credentials,
                  context.credential_owner}
               )

               :done
             end)

    assert_receive {:owned, supervisor, tasks, credentials, provider}
    for pid <- [supervisor, tasks, credentials, provider], do: refute(Process.alive?(pid))
  end

  test "fresh invocation starts HTTP dependencies without storage servers", %{tmp_dir: root} do
    # Umbrella Mix starts server applications before tests; isolate startup in a
    # fresh VM with only the standalone CLI application's dependency code paths.
    paths =
      application_closure([:ex_storage_service_cli], MapSet.new())
      |> Enum.flat_map(&["-pa", Application.app_dir(&1, "ebin")])

    script = """
    alias ExStorageServiceCli.CLI.Runtime
    forbidden = [:ex_storage_service, :ex_storage_service_web, :ex_storage_service_s3,
                 :ex_storage_service_cluster, :concord, :bandit]
    assert_no_servers = fn ->
      apps = Enum.map(Application.started_applications(), &elem(&1, 0))
      [] = Enum.filter(apps, &(&1 in forbidden))
      apps
    end
    apps = assert_no_servers.()
    false = :http_fetch in apps
    :ok = Runtime.with_invocation(fn runtime ->
      true = :http_fetch in assert_no_servers.()
      true = is_pid(Runtime.task_supervisor(runtime))
      {:ok, context} = Runtime.aws_context(runtime, %{},
        env: %{"AWS_ACCESS_KEY_ID" => "a", "AWS_SECRET_ACCESS_KEY" => "s"},
        home: #{inspect(root)})
      true = is_pid(context.credential_owner)
      assert_no_servers.()
      :ok
    end)
    assert_no_servers.()
    IO.puts("STANDALONE_HTTP_ONLY")
    """

    assert {output, 0} =
             System.cmd(
               System.find_executable("elixir"),
               ["--erl", "+S 2:2"] ++ paths ++ ["-e", script],
               stderr_to_stdout: true
             )

    assert output =~ "STANDALONE_HTTP_ONLY"
  end

  test "provider refresh updates session credentials before subsequent signatures", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], response("first", "2026-10-15T01:00:00Z")},
        {200, [], response("second", "2026-10-15T02:00:00Z")}
      ])

    {:ok, clock} = Agent.start_link(fn -> @now end)

    opts = [
      env: %{"AWS_CONTAINER_CREDENTIALS_FULL_URI" => endpoint},
      home: root,
      now: @now,
      clock: fn -> Agent.get(clock, & &1) end
    ]

    assert :ok =
             Runtime.with_invocation(fn runtime ->
               assert {:ok,
                       %Context{access_key_id: "first", session_token: "first-token"} = context} =
                        Runtime.aws_context(runtime, %{}, opts)

               Agent.update(clock, fn _ -> ~U[2026-10-15 00:56:00Z] end)

               assert {:ok,
                       %Context{
                         access_key_id: "second",
                         session_token: "second-token",
                         credential_owner: owner
                       }} = Context.refresh(context)

               assert owner == context.credential_owner
               assert_receive {:wire_request, _}
               assert_receive {:wire_request, _}
               :ok
             end)
  end

  test "native AWS aliases own independent provider processes", %{tmp_dir: root} do
    config_file = Path.join(root, "config")
    credentials = Path.join(root, "credentials")

    File.write!(
      config_file,
      "[profile first]\nregion=us-west-1\n[profile second]\nregion=eu-west-1\n"
    )

    File.write!(
      credentials,
      "[first]\naws_access_key_id=first-a\naws_secret_access_key=first-s\n[second]\naws_access_key_id=second-a\naws_secret_access_key=second-s\n"
    )

    config = %{
      "aliases" => %{
        "first" => %{
          "endpoint" => "https://first.test",
          "provider" => "aws-profile",
          "profile" => "first"
        },
        "second" => %{
          "endpoint" => "https://second.test",
          "provider" => "aws-profile",
          "profile" => "second"
        }
      }
    }

    Runtime.with_invocation(fn runtime ->
      opts = [
        runtime: runtime,
        env: %{},
        home: root,
        config_file: config_file,
        credentials_file: credentials
      ]

      assert {:ok, first} = Context.native(%Target{alias: "first"}, %{}, config, opts)
      assert {:ok, second} = Context.native(%Target{alias: "second"}, %{}, config, opts)
      assert first.credential_owner != second.credential_owner
      assert first.access_key_id == "first-a"
      assert second.access_key_id == "second-a"
      assert first.endpoint == "https://first.test"
      assert second.endpoint == "https://second.test"
    end)
  end

  test "unsigned contexts allocate no credential owner or network work", %{tmp_dir: root} do
    File.write!(Path.join(root, "config"), "[default]\nregion=us-east-1\n")
    File.write!(Path.join(root, "credentials"), "malformed")

    Runtime.with_invocation(fn runtime ->
      assert {:ok, %Context{unsigned: true, credential_owner: nil}} =
               Runtime.aws_context(runtime, %{no_sign_request: true},
                 env: %{},
                 home: root,
                 config_file: Path.join(root, "config"),
                 credentials_file: Path.join(root, "credentials")
               )

      assert DynamicSupervisor.count_children(runtime.credentials).active == 0
    end)
  end

  test "callback failures still stop the owned invocation tree" do
    parent = self()

    assert_raise RuntimeError, "fixture", fn ->
      Runtime.with_invocation(fn runtime ->
        send(parent, {:supervisor, runtime.supervisor})
        raise "fixture"
      end)
    end

    assert_receive {:supervisor, supervisor}
    refute Process.alive?(supervisor)
  end

  defp application_closure([], seen), do: seen

  defp application_closure([app | rest], seen) do
    if MapSet.member?(seen, app) do
      application_closure(rest, seen)
    else
      assert Application.load(app) in [:ok, {:error, {:already_loaded, app}}]
      optional = Application.spec(app, :optional_applications) || []

      dependencies =
        Enum.filter(Application.spec(app, :applications) || [], fn dependency ->
          dependency not in optional or
            :code.where_is_file(String.to_charlist("#{dependency}.app")) != :non_existing
        end)

      application_closure(dependencies ++ rest, MapSet.put(seen, app))
    end
  end

  defp response(access, expiration),
    do:
      JSON.encode!(%{
        "AccessKeyId" => access,
        "SecretAccessKey" => "secret",
        "Token" => access <> "-token",
        "Expiration" => expiration
      })
end

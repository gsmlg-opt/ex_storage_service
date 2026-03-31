defmodule ExStorageServiceWeb.SessionControllerTest do
  use ExStorageServiceWeb.ConnCase

  @admin_user "admin"
  @admin_password "admin"

  describe "GET /login" do
    test "renders the login page", %{conn: conn} do
      conn = get(conn, "/login")
      assert html_response(conn, 200) =~ "Admin Login"
    end
  end

  describe "POST /login" do
    test "with correct credentials redirects to /dashboard", %{conn: conn} do
      conn =
        post(conn, "/login", %{
          "username" => @admin_user,
          "password" => @admin_password
        })

      assert redirected_to(conn, 302) == "/dashboard"
    end

    test "with wrong password re-renders login with error", %{conn: conn} do
      conn =
        post(conn, "/login", %{
          "username" => @admin_user,
          "password" => "wrong_password"
        })

      response = html_response(conn, 200)
      assert response =~ "Invalid username or password"
    end

    test "with wrong username re-renders login with error", %{conn: conn} do
      conn =
        post(conn, "/login", %{
          "username" => "not_admin",
          "password" => @admin_password
        })

      response = html_response(conn, 200)
      assert response =~ "Invalid username or password"
    end
  end

  describe "protected routes without auth" do
    test "GET /dashboard redirects to /login", %{conn: conn} do
      conn = get(conn, "/dashboard")
      assert redirected_to(conn, 302) =~ "/login"
    end

    test "GET /users redirects to /login", %{conn: conn} do
      conn = get(conn, "/users")
      assert redirected_to(conn, 302) =~ "/login"
    end

    test "GET /policies redirects to /login", %{conn: conn} do
      conn = get(conn, "/policies")
      assert redirected_to(conn, 302) =~ "/login"
    end

    test "GET /buckets redirects to /login", %{conn: conn} do
      conn = get(conn, "/buckets")
      assert redirected_to(conn, 302) =~ "/login"
    end

    test "GET /audit redirects to /login", %{conn: conn} do
      conn = get(conn, "/audit")
      assert redirected_to(conn, 302) =~ "/login"
    end
  end

  describe "DELETE /logout" do
    test "clears session and redirects to /login", %{conn: conn} do
      # Log in first
      conn =
        post(conn, "/login", %{
          "username" => @admin_user,
          "password" => @admin_password
        })

      # Logout
      conn =
        conn
        |> recycle()
        |> delete("/logout")

      assert redirected_to(conn, 302) =~ "/login"

      # Verify session is cleared
      conn =
        conn
        |> recycle()
        |> get("/dashboard")

      assert redirected_to(conn, 302) =~ "/login"
    end
  end
end

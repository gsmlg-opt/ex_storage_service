Application.put_env(:ex_storage_service, :root_admin_user, "admin")

# CI's production startup hash must not replace the web suite's fixture password.
Application.put_env(
  :ex_storage_service,
  :root_admin_password_hash,
  Base.encode16(:crypto.hash(:sha256, "admin"), case: :lower)
)

ExUnit.start()

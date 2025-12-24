from eclipse24.libs.stores.narcissus_client import servers_with_flag

if __name__ == "__main__":
    servers = servers_with_flag("conf_frontend_log_level")
    print(f"Total: {len(servers)}")
    for name in servers[:25]:
        print("-", name)

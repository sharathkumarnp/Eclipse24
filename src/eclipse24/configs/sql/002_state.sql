-- src/eclipse24/configs/sql/002_state.sql
-- ===========================================
-- Enhancements and indexes for Safeguard DB
-- ===========================================

-- Indexes
create index if not exists idx_server_state_status on server_state(status);
create index if not exists idx_server_state_until on server_state(until);
create index if not exists idx_audit_log_server_id on audit_log(server_id);
create index if not exists idx_audit_log_created_at on audit_log(created_at desc);

-- Helpful view for quick inspection
create
or replace view v_server_state_summary as
select s.server_id,
       s.server_name,
       s.owner_email,
       st.status,
       st.until,
       st.updated_at,
       (select max(a.created_at) from audit_log a where a.server_id = s.server_id) as last_action_at
from servers s
         left join server_state st on s.server_id = st.server_id;
USE nyartcc_nyartcco;
SELECT COUNT(cid) FROM controllers WHERE status = 1;    # Status 1: Members
SELECT COUNT(cid) FROM controllers WHERE status = 2;    # Status 2: Visitors
SELECT COUNT(cid) FROM controllers WHERE status < 3 AND loa=1;  # Members on LOA.

# STAFF
SELECT COUNT(cid) FROM controllers WHERE status < 3 AND staff > 0 AND staff <= 6 AND cid!=7;
SELECT COUNT(cid) FROM controllers WHERE status < 3 AND instructor = 2;
SELECT COUNT(cid) FROM controllers WHERE status < 3 AND instructor = 1;
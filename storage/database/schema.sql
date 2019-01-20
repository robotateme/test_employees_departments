drop table if exists employees_departments;
create table if not exists employees_departments
(
  id        INTEGER not null
    primary key
  autoincrement,
  Name      TEXT    not null,
  Type      INTEGER not null,
  ParentId INTEGER
    constraint employees_departments_parent_fk
    references employees_departments (id)
      on update cascade
      on delete cascade
);

CREATE INDEX parent_id ON employees_departments (ParentId)
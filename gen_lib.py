import re

def create_tables_base():
    return ('CREATE TABLE Object (\n'
            '  id          SERIAL,\n'
            '  time_create TIMESTAMP,\n'
            '  time_dead   TIMESTAMP DEFAULT make_timestamp(3999, 1, 1, 0, 0, 0),\n'
            '  deleted     BOOLEAN   DEFAULT FALSE,\n'
            '  PRIMARY KEY (id, time_create)\n'
            ');\n'
            '\n'
            'CREATE TABLE Journal (\n'
            '  id      INT,\n'
            '  time    TIMESTAMP,\n'
            '  op_name VARCHAR(10),\n'
            '  deleted BOOLEAN DEFAULT FALSE,\n'
            '  PRIMARY KEY (id, time)\n'
            ');\n\n')

def create_tables(tables):

    command_temp = ('CREATE TABLE {0}(\n'
                   '{1}\n'
                   ')\n'
                   '  INHERITS (Object);')

    commands = ''

    for table in tables:
        table_name = table[0]
        rows = ''
        first_row = True

        for column in table[1]:
            col_str = ''

            find_pattern = r"REFERENCES .*"
            old_ref = re.findall(find_pattern, column[1])

            if old_ref:
                col_type = re.sub(find_pattern, '', column[1])
            else:
                col_type = column[1]

            if first_row:
                col_str += '\t' + column[0] + ' ' + col_type
                first_row = False
            else:
                col_str += ',\n\t' + column[0] + ' ' + col_type

            rows += col_str

        commands += command_temp.format(table_name, rows) + '\n'*2

    return commands

def create_tables_temp(tables):
    command_temp = ('CREATE TABLE {0}_temp (\n'
                    '\tid   INT PRIMARY KEY,\n'
                    '{1}'
                    ');')

    commands = ''

    for table in tables:
        table_name = table[0]
        rows = ''
        first_row = True

        for column in table[1]:
            col_str = ''

            if first_row:
                col_str += '\t' + column[0] + ' ' + column[1]
                first_row = False
            else:
                col_str += ',\n\t' + column[0] + ' ' + column[1]

            rows += col_str

        commands += command_temp.format(table_name, rows) + '\n'*2

    return commands

def create_ops_triggers(tables):

    insert_temp = "(iCurrID, tTime_create, DEFAULT, new.deleted, {0})"

    tr_set_time_temp = ("CREATE OR REPLACE FUNCTION {0}_set_time()\n"
                        "  RETURNS TRIGGER AS\n"
                        "$$\n"
                        "DECLARE\n"
                        "  tTime_create  TIMESTAMP;\n"
                        "  tTime_dead    TIMESTAMP;\n"
                        "  tTime_DEFAULT TIMESTAMP;\n"
                        "  iCurrID       INT;\n"
                        "\n"
                        "BEGIN\n"
                        "  tTime_DEFAULT = make_timestamp(3999, 1, 1, 0, 0, 0);\n"
                        "\n"
                        "  IF (tg_op = 'INSERT')\n"
                        "  THEN\n"
                        "    IF (new.id NOT IN (SELECT id\n"
                        "                       FROM {0}))\n"
                        "    THEN\n"
                        "\n"
                        "      tTime_create = clock_timestamp();\n"
                        "      NEW.time_create = tTime_create;\n"
                        "\n"
                        "      INSERT INTO Journal VALUES (new.id, new.time_create, 'INSERT');\n"
                        "\n"
                        "      RETURN new;\n"
                        "\n"
                        "    ELSE\n"
                        "      RETURN new;\n"
                        "    END IF;\n"
                        "\n"
                        "  ELSEIF (tg_op = 'UPDATE')\n"
                        "    THEN\n"
                        "      IF (new.deleted = TRUE)\n"
                        "      THEN\n"
                        "        old.deleted = NOT old.deleted;\n"
                        "        RETURN old;\n"
                        "\n"
                        "      ELSE\n"
                        "        IF (NEW.time_dead = tTime_DEFAULT)\n"
                        "        THEN\n"
                        "\n"
                        "          iCurrID = old.id;\n"
                        "          tTime_dead = clock_timestamp();\n"
                        "          tTime_create = tTime_dead;\n"
                        "\n"
                        "          old.id = iCurrID;\n"
                        "          old.time_dead = tTime_dead;\n"
                        "\n"
                        "          INSERT INTO {0}\n"
                        "          VALUES {1};\n"
                        "\n"
                        "          INSERT INTO Journal VALUES (iCurrID, tTime_dead, 'UPDATE');\n"
                        "\n"
                        "          RETURN old;\n"
                        "\n"
                        "        ELSEIF (old.time_dead = tTime_DEFAULT)\n"
                        "          THEN\n"
                        "\n"
                        "            old.time_dead = new.time_dead;\n"
                        "\n"
                        "            RETURN old;\n"
                        "\n"
                        "        END IF;\n"
                        "      END IF;\n"
                        "\n"
                        "  ELSEIF (tg_op = 'DELETE')\n"
                        "    THEN\n"
                        "      IF (old.time_dead = tTime_DEFAULT)\n"
                        "      THEN\n"
                        "\n"
                        "        tTime_dead = clock_timestamp();\n"
                        "        iCurrID = old.id;\n"
                        "        tTime_create = old.time_create;\n"
                        "\n"
                        "        UPDATE {0}\n"
                        "        SET time_dead = tTime_dead\n"
                        "        WHERE id = iCurrID;\n"
                        "\n"
                        "        INSERT INTO Journal VALUES (iCurrID, tTime_dead, 'DELETE');\n"
                        "\n"
                        "        RETURN NULL;\n"
                        "\n"
                        "      END IF;\n"
                        "\n"
                        "  END IF;\n"
                        "\n"
                        "  RETURN NULL;\n"
                        "END;\n"
                        "$$\n"
                        "LANGUAGE plpgsql;\n"
                        "\n"
                        "CREATE TRIGGER {0}_trigger\n"
                        "  BEFORE INSERT OR UPDATE OR DELETE\n"
                        "  ON {0}\n"
                        "  FOR EACH ROW EXECUTE PROCEDURE {0}_set_time();")

    commands = ''

    for table in tables:
        table_name = table[0]

        ins_row = ''
        first_row = True

        for row in table[1]:
            if first_row:
                ins_row += 'NEW.' + row[0]
                first_row = False
            else:
                ins_row += ', NEW.' + row[0]

        insert_command = insert_temp.format(ins_row)

        commands += tr_set_time_temp.format(table_name, insert_command) + '\n\n'

    return commands

def create_temp_triggers(tables):

    select_temp = """SELECT DISTINCT
                  id,
                  {1}
                FROM {0} t, (SELECT max(time_create) AS time_create
                                 FROM {0}
                                 WHERE deleted = FALSE
                                 GROUP BY id) time
                WHERE (t.time_create = time.time_create) AND
                      (t.time_dead = make_timestamp(3999, 1, 1, 0, 0, 0)) AND
                      (t.deleted = FALSE)"""

    insert_temp = "(data_row.id, {0})"
    update_temp = "{0}"

    tr_update_temp_temp = """CREATE OR REPLACE FUNCTION update_{0}_temp()
  RETURNS TRIGGER AS
$$
DECLARE
  data_row RECORD;

BEGIN

  /* Insert or update artist in artist_temp */

  FOR data_row IN {1} LOOP

    IF (data_row.id NOT IN (SELECT id
                          FROM {0}_temp))
    THEN
      INSERT INTO {0}_temp VALUES {2};

    ELSE
      UPDATE {0}_temp
      SET {3}
      WHERE id = data_row.id;

    END IF;

  END LOOP;

  /* Delete artists from artists_temp */

  FOR data_row IN SELECT id
                FROM {0}_temp LOOP
    IF (data_row.id NOT IN (SELECT DISTINCT id
                         FROM {0} t, (SELECT max(time_create) AS time_create
                                         FROM {0}
                                         WHERE deleted = FALSE
                                         GROUP BY id) time
                         WHERE (t.time_create = time.time_create) AND
                               (t.time_dead = make_timestamp(3999, 1, 1, 0, 0, 0)) AND
                               (t.deleted = FALSE)))
    THEN
      DELETE FROM {0}_temp
      WHERE {0}_temp.id = data_row.id;
    END IF;

  END LOOP;

  RETURN NULL;

END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER {0}_after_trigger
  AFTER INSERT OR UPDATE OR DELETE
  ON {0}
  FOR EACH STATEMENT EXECUTE PROCEDURE update_{0}_temp();"""

    commands = ''

    for table in tables:
        table_name = table[0]

        ins_row = ''
        sel_row = ''
        upd_row = ''
        first_row = True

        for row in table[1]:
            if first_row:
                sel_row += row[0]
                ins_row += 'data_row.' + row[0]
                upd_row += row[0] + ' = data_row.' + row[0]
                first_row = False
            else:
                sel_row += ',\n' + '\t'*5 + row[0]
                ins_row += ', ' + 'data_row.' + row[0]
                upd_row += ',\n\t\t\t\t' + row[0] + ' = data_row.' + row[0]

        select_row = select_temp.format(table_name, sel_row)
        insert_row = insert_temp.format(ins_row)
        update_row = update_temp.format(upd_row)

        commands += tr_update_temp_temp.format(table_name,
                                               select_row,
                                               insert_row,
                                               update_row) + '\n\n'
    return commands

def create_cancel_ops(tables):
    cancel_op_temp = """CREATE OR REPLACE FUNCTION cancel_ops(ops_count INT)
  RETURNS VOID AS
$$
DECLARE
    get_journal CURSOR FOR SELECT *
                           FROM Journal
                           WHERE deleted = FALSE
                           ORDER BY time DESC
                           LIMIT ops_count;

  journal_row   RECORD;
  tTime_DEFAULT TIMESTAMP;

BEGIN
  tTime_DEFAULT = make_timestamp(3999, 1, 1, 0, 0, 0);

  {0}
  OPEN get_journal;

  LOOP
    FETCH get_journal INTO journal_row;
    EXIT WHEN journal_row IS NULL;

    {1}END IF;
    
  END LOOP;

  CLOSE get_journal;

  {2}
END;
$$
LANGUAGE plpgsql;"""

    table_ops_temp = """IF (journal_row.id IN (SELECT id
                           FROM {0}))
    THEN
      IF (journal_row.op_name = 'INSERT')
      THEN
        UPDATE {0}
        SET deleted = TRUE
        WHERE time_create = journal_row.time AND time_dead = tTime_DEFAULT;

        UPDATE Journal
        SET deleted = TRUE
        WHERE time = journal_row.time;

      ELSEIF (journal_row.op_name = 'UPDATE')
        THEN
          UPDATE {0}
          SET deleted = TRUE
          WHERE time_create = journal_row.time AND time_dead = tTime_DEFAULT;

          UPDATE {0}
          SET time_dead = tTime_DEFAULT
          WHERE time_dead = journal_row.time;

          UPDATE Journal
          SET deleted = TRUE
          WHERE time = journal_row.time;

      ELSEIF (journal_row.op_name = 'DELETE')
        THEN
          UPDATE {0}
          SET time_dead = tTime_DEFAULT
          WHERE time_dead = journal_row.time;

          UPDATE Journal
          SET deleted = TRUE
          WHERE time = journal_row.time;

      END IF;
"""

    disable_tr_temp = """ALTER TABLE {0}
    DISABLE TRIGGER {0}_trigger;"""

    enable_tr_temp = """ALTER TABLE {0}
    ENABLE TRIGGER ALL;"""

    table_operations = ''
    disable_triggers = ''
    enable_triggers = ''
    first_table = True

    for table in tables:
        table_name = table[0]

        disable_triggers += disable_tr_temp.format(table_name) +'\n'
        enable_triggers += enable_tr_temp.format(table_name) + '\n'

        if first_table:
            table_operations += table_ops_temp.format(table_name) + '\n\n'
            first_table = False
        else:
            table_operations += 'ELSE' + table_ops_temp.format(table_name) + '\n\n'

    return cancel_op_temp.format(disable_triggers,
                                 table_operations,
                                 enable_triggers)

def create_restore_ops(tables):
    restore_op_temp = """CREATE OR REPLACE FUNCTION restore_ops(ops_count INT)
      RETURNS VOID AS
    $$
    DECLARE
        get_journal CURSOR FOR SELECT *
                               FROM Journal
                               WHERE deleted = TRUE 
                               ORDER BY time
                               LIMIT ops_count;

      journal_row   RECORD;
      tTime_DEFAULT TIMESTAMP;

    BEGIN
      tTime_DEFAULT = make_timestamp(3999, 1, 1, 0, 0, 0);

      {0}
      OPEN get_journal;

      LOOP
        FETCH get_journal INTO journal_row;
        EXIT WHEN journal_row IS NULL;

        {1}END IF;

      END LOOP;

      CLOSE get_journal;

      {2}
    END;
    $$
    LANGUAGE plpgsql;"""

    table_ops_temp = """IF (journal_row.id IN (SELECT id
                           FROM {0}))
    THEN

      IF (journal_row.op_name = 'INSERT')
      THEN
        UPDATE {0}
        SET deleted = FALSE
        WHERE time_create = journal_row.time AND time_dead = tTime_DEFAULT;

        UPDATE Journal
        SET deleted = FALSE
        WHERE time = journal_row.time;

      ELSEIF (journal_row.op_name = 'UPDATE')
        THEN
          UPDATE {0}
          SET time_dead = journal_row.time
          WHERE time_dead = tTime_DEFAULT AND
                id = journal_row.id AND
                deleted = FALSE;

          UPDATE {0}
          SET deleted = FALSE
          WHERE time_create = journal_row.time AND time_dead = tTime_DEFAULT;

          UPDATE Journal
          SET deleted = FALSE
          WHERE time = journal_row.time;

      ELSEIF (journal_row.op_name = 'DELETE')
        THEN
          UPDATE {0}
          SET time_dead = journal_row.time
          WHERE time_dead = tTime_DEFAULT AND id = journal_row.id;

          UPDATE Journal
          SET deleted = FALSE
          WHERE time = journal_row.time;

      END IF;"""

    disable_tr_temp = """ALTER TABLE {0}
        DISABLE TRIGGER {0}_trigger;"""

    enable_tr_temp = """ALTER TABLE {0}
        ENABLE TRIGGER ALL;"""

    table_operations = ''
    disable_triggers = ''
    enable_triggers = ''
    first_table = True

    for table in tables:
        table_name = table[0]

        disable_triggers += disable_tr_temp.format(table_name) + '\n'
        enable_triggers += enable_tr_temp.format(table_name) + '\n'

        if first_table:
            table_operations += table_ops_temp.format(table_name) + '\n\n'
            first_table = False
        else:
            table_operations += 'ELSE' + table_ops_temp.format(table_name) + '\n\n'

    return restore_op_temp.format(disable_triggers,
                                 table_operations,
                                 enable_triggers)

def create_time_ops():
    commands = """CREATE OR REPLACE FUNCTION time_cancel_ops(cancel_time TIMESTAMP)
  RETURNS VOID AS
$$
DECLARE
  iOpsCount INT;
BEGIN
  SELECT INTO iOpsCount count(time)
  FROM journal
  WHERE time BETWEEN cancel_time AND make_timestamp(3999, 1, 1, 0, 0, 0) AND
        deleted = FALSE;

  PERFORM cancel_ops(iOpsCount);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION time_restore_ops(cancel_time TIMESTAMP)
  RETURNS VOID AS
$$
DECLARE
  iOpsCount INT;
BEGIN
  SELECT INTO iOpsCount count(time)
  FROM journal
  WHERE time <= cancel_time AND
        deleted = TRUE;

  PERFORM restore_ops(iOpsCount);
END;
$$
LANGUAGE plpgsql;"""

    return commands

# Summary

def create_database(tables):
    listing = ''

    listing += create_tables_base() + '\n\n'
    listing += create_tables(tables) + '\n\n'
    listing += create_tables_temp(tables) + '\n\n'
    listing += create_ops_triggers(tables) + '\n\n'
    listing += create_temp_triggers(tables) + '\n\n'
    listing += create_cancel_ops(tables) + '\n\n'
    listing += create_restore_ops(tables) + '\n\n'
    listing += create_time_ops()

    return listing

# Misc

def rename_reference(tables):
    find_pattern = r"REFERENCES (.*)\("

    for table in tables:
        for column in table[1]:
            old_ref = re.findall(find_pattern, column[1])

            if old_ref:
                old_ref = old_ref[0]
                result = re.sub(old_ref, old_ref + '_temp', column[1])
                column[1] = result

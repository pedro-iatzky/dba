from sqlalchemy.exc import ProgrammingError


def execute_or_pass(db, sentence):
    try:
        db.execute(sentence)
        db.commit()
    except ProgrammingError:
        db.rollback()


def execute_or_fail(db, sentence):
    try:
        db.execute(sentence)
        db.commit()
    except Exception:
        db.rollback()
        raise Exception


class Sentences(object):

    @staticmethod
    def change_password(role_name, new_password):
        return "ALTER ROLE \"{}\" WITH PASSWORD '{}'".format(role_name, new_password)

    @staticmethod
    def grant_permissions_on_all_tables(role_name, permissions):
        """

        :param role_name: <str>
        :param permissions: <tuple> or <list>. Eg. ('SELECT', 'UPDATE', 'INSERT')
        :return:
        """
        permissions_str = ', '.join(permissions)
        return ("GRANT {} ON ALL TABLES IN SCHEMA public TO '{}'"
                .format(permissions_str, role_name))

    @staticmethod
    def grant_permissions_on_all_sequences(role_name):
        return ("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"{}\""
                .format(role_name))

    @staticmethod
    def alter_def_permissions_on_all_sequences(role_name):
        return ('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE,'
                ' SELECT ON SEQUENCES TO "{}"'.format(role_name))

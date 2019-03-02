#!/usr/bin/env bash

# This script is meant to make a copy of a postgresql Database from one
# server to another one.
# If you want to create a copy  into the same server do not use this
# script for that purpose!!
# You just have to execute (directly in SQL)
# "CREATE DATABASE your_db_copy TEMPLATE your_db_to_be_copied",

# replicate_database.sh --source_host <your source host> --destination_host
# <your destination host> --source_db <the database you want to copy>

# We assume the "postgres" database always exists. We need an existent database for
# testing the connections parameters

DESTINATION_EXISTENT_DB=postgres
# We can set an avoided destination host. For example for assuring not
# to write a production server
AVOIDED_DESTINATION_HOST="your_production_host"
TEMP_FOLDER="/tmp"

DEVELOPER_ROLE="Developer"
VIEWER_ROLE="Viewer"
# IF the roles do not exist we will create them with a default password.
# This one can be modified later
DEFAULT_PASSWORD="123456"

# default port values

SOURCE_PORT=5432
DESTINATION_PORT=5432

usage() {
    echo "usage: replicate_database [[[-sh source_host ] [-sp source_port] " \
     "[-dh destination_host] [-dp destination_port] [-db source_db] ] | [-h help]]"
    }

while [ "$1" != "" ]; do
    case $1 in
        -sh | --source_host )      shift
                                   SOURCE_HOST=$1
                                   ;;
        -sp | --source_port )      shift
                                       SOURCE_PORT=$1
                                   ;;

        -dh | --destination_host ) shift
                                   if [ $1 = "$AVOIDED_DESTINATION_HOST" ]; then
                                        echo "The $AVOIDED_DESTINATION_HOST destination" \
                                         "host is not elegible as a destination host"
                                        exit 1
                                   fi
                                   DESTINATION_HOST=$1
                                   ;;
        -dp | --destination_port ) shift
                                   DESTINATION_PORT=$1
                                   ;;
        -db | --source_db )        shift
                                   SOURCE_DB=$1
                                   ;;
        -h | --help )              usage
                                   exit
                                   ;;
        * )                        usage
                                   exit 1
    esac
    shift
done

if [ -z "$SOURCE_HOST" ]; then
    echo "you must specify the source_host"
    exit 1
elif [ -z "$DESTINATION_HOST" ]; then
    echo "you must specify the destination_host"
    exit 1
elif [ -z "$SOURCE_DB" ]; then
    echo "you must specify the source database"
    exit 1
fi

credentials_destination=(destination_user destination_password)


check_package_install_if_not() {
    # check if the input package is installed. If not, install it
    dpkg -s $1 &> /dev/null
    if [ $? -ne 0  ]; then
        echo "$1 not installed. Installing..."
        sudo apt-get update
        sudo apt-get install $1
    else
        echo "$1 is already installed"
     fi
}

set_up_tools() {
    # check psql and pg_dump are installed. If not, install it
    # TODO: solve version incompatibilities
    check_package_install_if_not postgresql-client
}

check_no_null() {
        if [ -n "$response" ]; then
            exit 2
        fi
        }

set_connection_password() {
    export PGPASSWORD=$1
    }

test_connection_credentials() {
# The test below should be improved

        # issue a command only to test the connection
        set_connection_password $2
        echo "Testing authentication..."
        response=$($1 -l)
        if [ "$?" != 0 ]; then
            echo "Aborting..."
            exit 1
        fi
        echo "Testing Administrator role..."
        response=$($1 -c "\\du" | grep $3 | grep "Create DB")
        if [ "$?" != 0 ]; then
            echo "The credentials you input do not have super user permissions.
            You will need to provide credentials with administration permission.
            Aborting..."
            exit 2
        fi
        }


check_role_exists(){

# The test below should be improved
#        $1: command : <str>
#        $2: RoleName: <str>
#        $3: RolePassword: <str>
#       return: 0 if the role exists, 2 if it does not exist
        # issue a command only to check if a role already exists
        set_connection_password $3
        echo "Checking if the role $2 exists in the server..."
        response=$($1 -c "\\du" | grep $2)
        if [ "$?" != 0 ]; then
            echo "The role $2... does not exist in the database server. Please Create it"
            exit 2
        fi
        }


request_connection_credentials() {
    # first test the connection with the source server
    echo "input the USER for the SOURCE HOST, i.e. the one with the database"\
     "you want to copy from:"
    read response
    source_user=$response

    echo "input the PASSWORD for the SOURCE HOST user:"
    read -s response
    source_password=$response

    source_command="psql -d $SOURCE_DB -h $SOURCE_HOST -p $SOURCE_PORT -U $source_user"
    test_connection_credentials "$source_command" "$source_password" "$source_user"

    # Then test the connection with the destination server
    echo "input the USER for the DESTINATION HOST, i.e. the one to copy the database into:"
    read response
    destination_user=$response

    echo "input the PASSWORD for the DESTINATION HOST user:"
    read -s response
    destination_password=$response

    destination_command="psql -d $DESTINATION_EXISTENT_DB -h $DESTINATION_HOST -p $DESTINATION_PORT -U $destination_user"
    test_connection_credentials "$destination_command" "$destination_password" "$destination_user"
    }

dump_source_db() {
    # We are gonna dump the database into a local temporal file
    current_date_time=$(date -u --rfc-3339=seconds)
    current_date_time="${current_date_time//[" "":""-"]/"_"}"
    current_date_time="${current_date_time:0:-9}"

    set_connection_password $source_password
    DUMP_FILE=""$TEMP_FOLDER"/backup_"$SOURCE_DB"_"$current_date_time".sql"

    echo "dumping the database into the "$DUMP_FILE" file"
    pg_dump -d $SOURCE_DB -h $SOURCE_HOST -p $SOURCE_PORT -U $source_user -f "$DUMP_FILE" -O -x
    }

create_copy_db() {
    set_connection_password $destination_password
    # First we create the new database
    DB_COPY="copy_"$SOURCE_DB"_"$current_date_time""

    $destination_command -c "CREATE DATABASE \""$DB_COPY"\";" &&

    # Then, create the schema from the dump file

    echo "replicating the database with the name "$DB_COPY""

    psql -d $DB_COPY -h $DESTINATION_HOST -p $DESTINATION_PORT -U $destination_user -f "$DUMP_FILE"
    }


create_roles_if_they_do_not_exist() {
    # Create both the developer and the viewer roles, if they don't exist yet.
    # Assign a default password, if they do not exist yet. This password can be
    # changed later
    # TODO stay DRY (don't repeat yourself)

    echo "checking if the $DEVELOPER_ROLE user is already created in the server"
    response=$(check_role_exists "$destination_command" $DEVELOPER_ROLE $destination_password)
    if [ "$?" == 0 ]; then
        echo "The $DEVELOPER_ROLE user is already created in the server"
    else
        echo "The $DEVELOPER_ROLE user is not already created in the server."\
         "Creating it with the password $DEFAULT_PASSWORD..."
        $destination_command -c "CREATE USER \"$DEVELOPER_ROLE\" password \'$DEFAULT_PASSWORD\';"
    fi

    echo "checking if the $VIEWER_ROLE user is already created in the server"
    response=$(check_role_exists "$destination_command" $VIEWER_ROLE $destination_password)
    if [ "$?" == 0 ]; then
        echo "The $VIEWER_ROLE user is already created in the server"
    else
        echo "The $VIEWER_ROLE user is not already created in the server."\
         "Creating it with the password $DEFAULT_PASSWORD..."
        $destination_command -c "CREATE USER \"$VIEWER_ROLE\" password \'$DEFAULT_PASSWORD\';"
    fi
    }


set_the_roles_permission() {
    # Set the roles for the developer and the viewer role
    COPY_DB_COMMAND="psql -d $DB_COPY -h $DESTINATION_HOST -p $DESTINATION_PORT -U $destination_user"

    check_role_exists "$COPY_DB_COMMAND" $DEVELOPER_ROLE $destination_password
    echo "Granting permissions to the $DEVELOPER_ROLE User..."
    $COPY_DB_COMMAND -c "GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES" \
     "IN SCHEMA public TO \"$DEVELOPER_ROLE\";"
    $COPY_DB_COMMAND -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,"\
     "INSERT, UPDATE, DELETE ON TABLES TO \"$DEVELOPER_ROLE\";"

    # Grant permissions to the sequences as well
    $COPY_DB_COMMAND -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public"\
     "TO \"$DEVELOPER_ROLE\";"
    $COPY_DB_COMMAND -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE,"\
     "SELECT ON SEQUENCES TO \"$DEVELOPER_ROLE\";"

    check_role_exists "$COPY_DB_COMMAND" $VIEWER_ROLE $destination_password
    echo "Granting permissions to the $VIEWER_ROLE User..."
    $COPY_DB_COMMAND -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"$VIEWER_ROLE\";"
    $COPY_DB_COMMAND -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT"\
     "SELECT ON TABLES TO \"$VIEWER_ROLE\";"
    }

main() {
    # First, we need to assure that all the needed tools are installed. If they're not
    # install them then
    set_up_tools
    # First, all the server credentials will be prompted, and the connection with
    #  each one will be tested
    request_connection_credentials
    dump_source_db
    create_copy_db
#
#    create_roles_if_they_do_not_exist
#    set_the_roles_permission
}


main

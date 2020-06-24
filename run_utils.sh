#!/usr/bin/env bash

# Print execution date time
# shellcheck disable=SC2046
echo `date`

# Change directory into where email_notifer.py script is located.
echo "Changing into ~/curw_iot_email_notifer"

echo "Inside `pwd`"
cd curw_iot_email_notifer
echo "Inside `pwd`"

# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install PyMySQL using pip.
if [ ! -f "pusher.log" ]
then

    echo "Installing pytz"
    pip install pytz
    echo "Installing mysql"
    pip install mysql.connector
fi

# Run email_notifier.py script.
echo "Running Pusher.py. Logs Available in pusher.log file."
#cmd >>file.txt 2>&1
#Bash executes the redirects from left to right as follows:
#  >>file.txt: Open file.txt in append mode and redirect stdout there.
#  2>&1: Redirect stderr to "where stdout is currently going". In this case, that is a file opened in append mode.
#In other words, the &1 reuses the file descriptor which stdout currently uses.
python3 utils.py >> email_notifier.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
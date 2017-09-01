import os
from datetime import datetime
import sys
from time import sleep


def main():
    unbuffered_stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)
    while True:
        unbuffered_stdout.write('current time is {}'.format(datetime.now()).encode('utf-8'))
        unbuffered_stdout.write('\n'.encode('utf-8'))
        sleep(2)

if __name__ == '__main__':
    main()
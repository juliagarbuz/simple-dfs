import sys
import os
import random

# Set up:
PATH_TO_ROOT="../../"
INPUT_DATA_DIR = PATH_TO_ROOT + "input_data/"

NUM_COMMANDS_PER_FILE=1000
commands = {'w': "write", 'r': "read", 2: "ls", 3: "submit"}

NUM_FILES = 10
files = ["file" + str(i) + ".txt" for i in range(1, NUM_FILES+1)]

probabilities = {   "INIT_FILES": {'prob_write':1, 'prob_read': 0},
                    "RANDOM": {'prob_write':0.5, 'prob_read': 0.5},
                    "WRITE_HEAVY": {'prob_write':0.9, 'prob_read': 0.1},
                    "READ_HEAVY": {'prob_write':0.1, 'prob_read': 0.9}       }

def build_write_command(command_number):
    return commands['w'] + ", " + files[random.randint(0, NUM_FILES-1)] + ", " + "This is command number " + str(command_number) + "."

def build_read_command(command_number):
    return commands['r'] + ", " + files[random.randint(0, NUM_FILES-1)]

def init_file_contents():
    file_contents = ""
    for file in files:
        file_contents += commands['w'] + ", " + file + ", INITIAL WRITE" + "\n"
    return file_contents

def generate_file_contents(prob_write):
    file_contents = ""
    for i in range(NUM_COMMANDS_PER_FILE):
        command = ""
        prob = random.random()
        if prob < prob_write:
            command = build_write_command(i+1)
        else:
            command = build_read_command(i+1)
        file_contents += command + "\n"
    return file_contents

if not os.path.exists(INPUT_DATA_DIR):
    os.makedirs(INPUT_DATA_DIR)

for probability_type in probabilities:
    new_input_file = INPUT_DATA_DIR + probability_type + ".txt"

    if (probability_type == "INIT_FILES"):
        contents = init_file_contents()

    else:
        prob_write = probabilities[probability_type]["prob_write"]
        contents = generate_file_contents(prob_write)

    file = open(new_input_file, 'w')
    file.write(contents)

# print("WRITE:", write_probability)
# print("READ:", read_probability)

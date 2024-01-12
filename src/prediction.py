import os
import keyboard
import time
from statsmodels.iolib.smpickle import load_pickle


def print_chosen_values(chosen_values): 
    os.system('cls' if os.name == 'nt' else 'clear')  # Clear the screen
    print("Player:", chosen_values['name'])
    print("Temp:", chosen_values['temperature'])
    print("Odds: ", chosen_values['odds_for'])
    print("3 game average:", chosen_values['last_3_matches_average'])
    print()

def print_menu(names, selected_index, search_string = ""):
    os.system('cls' if os.name == 'nt' else 'clear')  # Clear the screen
    print("Filter: ", search_string)
    
    for i, name in enumerate(names):
        if i == selected_index - 6 or i == selected_index + 6:
            print('...')
        elif i < selected_index - 5 or i > selected_index + 5: 
            continue
        elif i == selected_index:
            print(f"> {name}")
        else:
            print(f"  {name}")

def select_name(names):
    selected_index = 0
    search_string = ""
    filtered_names = names
    print_menu(names, selected_index)
    while True:
        time.sleep(0.10)
        event = keyboard.read_event(suppress=True)
        if event.event_type != keyboard.KEY_DOWN :
            continue

        if event.name == 'down':
            selected_index = min(selected_index + 1, len(filtered_names) - 1)
        elif event.name == 'up':
            selected_index = max(selected_index - 1, 0)
        elif event.name == 'enter':
            return filtered_names[selected_index]
        elif event.name == 'backspace':
            # If backspace is pressed, remove the last character from user_input
            selected_index = 0
            search_string = search_string[:-1]
            filtered_names = [s for s in names if s.lower().startswith(search_string)]
        elif 'a' <= event.name <= 'z':
            # If the user types a letter (a-z), append it to user_input
            selected_index = 0
            search_string += event.name
            filtered_names = [s for s in names if s.lower().startswith(search_string)]
        print_menu(filtered_names, selected_index, search_string)

def print_number_menu(value, hint, chosen_values): 
    print_chosen_values(chosen_values)
    print(hint+":", value)
    print("(valid characters are 0-9 and .)")


def select_number(hint, chosen_values):
    value = ""
    print_number_menu(value, hint, chosen_values)
    while True:
        time.sleep(0.10)
        event = keyboard.read_event(suppress=True)
        if event.event_type != keyboard.KEY_DOWN :
            continue
        if event.name == 'enter':
            return float(value)
        elif event.name == 'backspace':
            # If backspace is pressed, remove the last character from user_input
            value = value[:-1]
        elif '0' <= event.name <= '9' or event.name == '.':
            # If the user types a letter (a-z), append it to user_input
            value += event.name
        print_number_menu(value, hint, chosen_values)
        
def predict(data_dir):
    new_results = load_pickle(f"{data_dir}/models/model1.pickle")
    names = [name[7:-1] for name in new_results.params.keys() if name.find(':') < 0 and name.startswith('name[T.')]

    #names = ["Alice", "Bob", "Charlie", "David", "Eve", "Alice", "Bob", "Charlie", "David", "Eve", "Alice", "Bob", "Charlie", "David", "Eve"]
    names.sort()
    g_player = select_name(names)

    pred_dict = {
        'name': g_player,
        'temperature': '---',
        'odds_for': '---',
        'last_3_matches_average': '---'
        }

    pred_dict['temperature'] = select_number("Input temperature", pred_dict)
    pred_dict['odds_for'] = select_number("Input odds for players team", pred_dict)
    pred_dict['last_3_matches_average'] = select_number("Input average points for players in last three matches", pred_dict)


    print_chosen_values(pred_dict)

    results = new_results.predict(pred_dict)
    print("Predicted point total:", results.values[0])


    
if __name__ == "__main__":
    predict("data")
import tkinter as tk
from tkinter import ttk
import subprocess
import traceback

def run_selected_script():
    selected_script = script_var.get()
    execute_script(selected_script)

def run_all_scripts():
    for script in scripts_to_run:
        execute_script(script)

def execute_script(script):
    try:
        result = subprocess.run(['python3', script], capture_output=True, text=True, check=True)
        output_textbox.tag_config('success', foreground='green')
        output_textbox.insert(tk.END, f'Successfully executed: {script}\n', 'success')
        output_textbox.insert(tk.END, result.stdout)
    except subprocess.CalledProcessError as e:
        output_textbox.tag_config('error', foreground='red')
        output_textbox.insert(tk.END, f'Error executing: {script}\n', 'error')
        output_textbox.insert(tk.END, e.stderr)
    except Exception as e:
        output_textbox.tag_config('error', foreground='red')
        output_textbox.insert(tk.END, f'Error in script {script}: {e}\n', 'error')
        output_textbox.insert(tk.END, traceback.format_exc())

# List of scripts you want to run
scripts_to_run = [ 'directory_creation.py',
                  'exploitation_zone.py', 'formatted_zone.py', 'persistent_zone.py', 'trusted_zone.py']

root = tk.Tk()
root.title("Run Python Scripts")

frame = ttk.Frame(root, padding="10")
frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Drop-down for selecting individual scripts
script_var = tk.StringVar()
script_var.set('Select a Script')
script_dropdown = ttk.OptionMenu(frame, script_var, *scripts_to_run)
script_dropdown.grid(row=0, column=0, sticky=tk.W)

# Button to run selected script
run_selected_button = ttk.Button(frame, text="Run Selected", command=run_selected_script)
run_selected_button.grid(row=0, column=1, sticky=tk.W)

# Button to run all scripts
run_all_button = ttk.Button(frame, text="Run All", command=run_all_scripts)
run_all_button.grid(row=0, column=2, sticky=tk.W)

# Textbox to show output
output_textbox = tk.Text(frame, wrap='word', width=50, height=15)
output_textbox.grid(row=1, columnspan=3, sticky=(tk.W, tk.E))

frame.columnconfigure(0, weight=1)
frame.columnconfigure(1, weight=1)
frame.columnconfigure(2, weight=1)
frame.rowconfigure(1, weight=1)

root.mainloop()

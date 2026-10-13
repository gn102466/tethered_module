import sys
import time
import pygame
import json
# Initialize pygame and its joystick module
pygame.init()
pygame.joystick.init()

# Check for connected joysticks
joystick_count = pygame.joystick.get_count()
if joystick_count == 0:
    print("Error: No joystick/controller detected. Please plug one in.")
    sys.exit()

# Connect to the first available joystick
joystick = pygame.joystick.Joystick(0)
joystick.init()

print(f"Successfully connected to: {joystick.get_name()}")
print(f"Number of Axes: {joystick.get_numaxes()}")
print(f"Number of Buttons: {joystick.get_numbuttons()}")
print(f"Number of Hats (D-pad): {joystick.get_numhats()}")
print("Press CTRL+C to exit.\n" + "-" * 40)

# Deadzone threshold to filter out tiny stick-drift movements
DEADZONE = 0.1  

try:
    while True:
        # Pygame requires pumping the event queue to update joystick states
        pygame.event.pump()

        # 1. Read Analog Axes (Sticks and Triggers)
        for i in range(joystick.get_numaxes()):
            axis_value = joystick.get_axis(i)
            # Filter out minor drift when the stick is resting
            if abs(axis_value) > DEADZONE:
                print(f"Axis {i} moved to: {axis_value:.2f}")

        # 2. Read Digital Buttons
        for i in range(joystick.get_numbuttons()):
            if joystick.get_button(i):
                print(f"Button {i} pressed!")

        # 3. Read Hats (D-Pad)
        for i in range(joystick.get_numhats()):
            hat_value = joystick.get_hat(i)  # Returns a tuple (x, y) like (-1, 1)
            if hat_value != (0, 0):
                print(f"D-Pad/Hat {i} position: {hat_value}")
                
                
            

        data_to_send = json.dumps({"type":'teleop',
                        "axes":[joystick.get_axis(i) for i in range(joystick.get_numaxes())],
                        "buttons":[joystick.get_button(i) for i in range(joystick.get_numbuttons())]
                        })
        print(data_to_send)
        # tcp_client.send_queue.put(pack_json(PKT_JSON, data_to_send))
        
        # Short delay to prevent terminal spam and CPU throttling
        time.sleep(0.1)

        
        

except KeyboardInterrupt:
    print("\nProgram stopped by user.")
finally:
    pygame.quit()

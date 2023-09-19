import cmd

class CustomCLI(cmd.Cmd):
    prompt = 'PQL> '

    def do_add(self, arg):
        """Add two numbers."""
        try:
            # Split the user input into two numbers
            num1, num2 = map(float, arg.split())
            result = num1 + num2
            result = int(result)
            print(f'Result: {result}')
        except ValueError:
            print('Invalid input. Please enter two numbers.')

    def do_exit(self, arg):
        """Exit the CLI."""
        return True

if __name__ == '__main__':
    cli = CustomCLI()
    cli.cmdloop('Welcome to the Custom CLI. Type "exit" to exit.')
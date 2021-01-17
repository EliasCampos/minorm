from datetime import datetime

from minorm.connectors import connector
from minorm.db_specs import SQLiteSpec
from minorm.fields import BooleanField, CharField, DateTimeField
from minorm.models import Model


def main():
    connector.connect(SQLiteSpec('todos.db'))
    init_tables()

    try:
        run_todo_app()
    except KeyboardInterrupt:
        print('\nExiting...')
    finally:
        connector.disconnect()


def run_todo_app():
    print_description()

    commands_with_params = {
        'search': search_items,
        'add': add_item,
        'edit': edit_item,
        'complete': complete_item,
        'remove': remove_item,
    }
    singles_commands = {
        'list': list_items,
        'clean': clean_items,
        'help': print_description,
    }

    while True:
        command = input("$")
        command_args = command.strip().split(maxsplit=1)
        if not command_args:
            continue

        command_name = command_args[0]
        if len(command_args) == 2:
            try:
                handler = commands_with_params[command_name]
            except KeyError:
                print("Unsupported command. Use 'help' to list available commands.")
            else:
                parameter = command_args[1]
                handler(parameter)
        else:
            try:
                handler = singles_commands[command_name]
            except KeyError:
                print("Unsupported command. Use 'help' to list available commands.")
            else:
                handler()


class TodoItem(Model):
    title = CharField(max_length=120)
    created_at = DateTimeField(default=datetime.now)
    is_done = BooleanField(default=False)

    def __str__(self):
        title = f'[DONE] {self.title}' if self.is_done else self.title
        return ' | '.join((str(self.pk), str(self.created_at), title))


def add_item(title):
    items_with_same_title = TodoItem.qs.filter(title=title)
    if items_with_same_title.exists():
        print("WARNING: duplicated one or more items with the title.")

    new_item = TodoItem.qs.create(title=title)
    print("Added new item:", new_item.pk, new_item.title)


def list_items():
    todos = TodoItem.qs
    if not todos.exists():
        print("No todos yet.")
    else:
        _display_items(todos.order_by('created_at'))


def search_items(title):
    found_todos = TodoItem.qs.filter(title__contains=title)
    if not found_todos.exists():
        print("No todos found.")
    else:
        print("Found next todos:")
        _display_items(found_todos)


def edit_item(params):
    param_parts = params.split(maxsplit=1)
    if len(param_parts) < 2:
        print("Missing a title.")
        return

    item = _get_by_id(param_parts[0])
    if item:
        item.title = param_parts[1]
        item.save()
        print('Modified:', item.pk, item.title)


def complete_item(item_id):
    item = _get_by_id(item_id)
    if item:
        item.is_done = True
        item.save()
        print('DONE:', item.title)


def remove_item(item_id):
    item = _get_by_id(item_id)
    if item:
        item.delete()
        print(f"Item removed!")


def clean_items():
    TodoItem.qs.filter(is_done=True).delete()
    print("Removed done todos.")


def print_description():
    description = """
    TODO application.

    Commands:

    * list - List all todos

    * search [title] - Search a todo by title

    * add [title] - Add a new todo

    * edit [id] [title] - Change a title of the todo

    * complete [id] - Mark the todo as done

    * remove [id] - Remove the todo with specified ID
    
    * clean - Remove all done todos
    
    * help - Display help
    """

    print(description.lstrip())


def _get_by_id(lookup_value):
    try:
        item_id = int(lookup_value)
    except ValueError:
        print("ID should be a number")
        return None

    try:
        item = TodoItem.qs.get(id=item_id)
    except TodoItem.DoesNotExists:
        print(f"Item with id {item_id} does not exists.")
        return None

    return item


def _display_items(items):
    print(" | ".join(("ID", "Created at", "Title")))
    for item in items:
        print(item)


def init_tables():
    create_sql = TodoItem.render_sql()
    create_if_required_sql = create_sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    with connector.cursor() as curr:
        curr.execute(create_if_required_sql)


if __name__ == '__main__':
    main()

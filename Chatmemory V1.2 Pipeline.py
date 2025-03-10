"""
title: ChatMemoryDB Tools
author: https://github.com/NineCheese (modified for SQLite)
version: 1.6.0
license: MIT
"""

import os
import sqlite3
import json
from typing import Callable, Any
import asyncio
import datetime
from pydantic import BaseModel, Field
import tarfile
import socket
import threading
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer

from blueprints.function_calling_blueprint import Pipeline as FunctionCallingBlueprint


class Pipeline(FunctionCallingBlueprint):
    class Valves(FunctionCallingBlueprint.Valves):
        USE_MEMORY: bool = Field(
            default=True, description="Enable or disable memory usage."
        )
        MEMORY_REFRESH_INTERVAL: int = Field(
            default=60,
            description="Interval in minutes to refresh and analyze memory data.",
        )
        DEBUG: bool = Field(default=True, description="Enable or disable debug mode.")

    class Tools:
        def __init__(self, pipeline):  # Added pipeline argument
            self.pipeline = pipeline  # Store pipeline reference
            self.valves = self.pipeline.valves  # Access valves through pipeline
            self.memory = MemoryFunctions(debug=self.valves.DEBUG)
            self.confirmation_pending = False

        async def handle_input(
            self,
            input_text: str,
            tag: str,
            user_wants_to_add: bool,
            llm_wants_to_add: bool,
            by: str,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            AUTOMATICALLY Summarize user input and enhance responses using memory data.

            :params input_text: The TEXT .
            :returns: The response considering memory data.
            """
            emitter = EventEmitter(__event_emitter__)

            print(
                f"DEBUG: handle_input - START - Input text: '{input_text[:50]}...'"
            )  # START DEBUG

            try:  # <---- ADD TRY BLOCK AROUND THE MAIN FUNCTION LOGIC

                if self.valves.DEBUG:
                    print(
                        f"DEBUG: handle_input - Debug mode is ON. Input: {input_text}"
                    )

                print(
                    "DEBUG: handle_input - Emitting 'Analyzing input' status (done=False)"
                )  # DEBUG
                await emitter.emit(
                    f"Analyzing input for memory: {input_text}",
                    status="memory_analysis",
                    done=False,
                )
                print(
                    "DEBUG: handle_input - Finished emitting 'Analyzing input' status (done=False)"
                )  # DEBUG

                if self.valves.USE_MEMORY:
                    print("DEBUG: handle_input - USE_MEMORY is True")  # DEBUG
                    # Assume 'by' is determined outside and 'tag' is selected by LLM
                    if tag not in self.memory.tag_options:
                        tag = "others"

                    if user_wants_to_add:
                        print(
                            "DEBUG: handle_input - user_wants_to_add is True, tag:",
                            tag,
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - Emitting 'User requested to add' status (done=False)"
                        )  # DEBUG
                        await emitter.emit(
                            description=f"User requested to add to memory with tag {tag}",
                            status="memory_update",
                            done=False,
                        )
                        print(
                            "DEBUG: handle_input - Finished emitting 'User requested to add' status (done=False)"
                        )  # DEBUG
                        self.memory.add_to_memory(tag, input_text, "user")
                        print(
                            "DEBUG: handle_input - memory.add_to_memory (user) call completed"
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - Emitting 'Memory addition completed' status (user, done=True)"
                        )  # DEBUG
                        await emitter.emit(  # Completion emitter for user add
                            description="Memory addition completed.",
                            status="memory_update",
                            done=True,
                        )
                        print(
                            "DEBUG: handle_input - Finished emitting 'Memory addition completed' status (user, done=True)"
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - RETURN: added to memory by user's request!"
                        )  # DEBUG
                        return "added to memory by user's request!"
                    elif llm_wants_to_add:
                        print(
                            "DEBUG: handle_input - llm_wants_to_add is True, tag:",
                            tag,
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - Emitting 'LLM added to memory' status (done=False)"
                        )  # DEBUG
                        await emitter.emit(
                            description=f"LLM added to memory with tag {tag}",
                            status="memory_update",
                            done=False,
                        )
                        print(
                            "DEBUG: handle_input - Finished emitting 'LLM added to memory' status (done=False)"
                        )  # DEBUG
                        self.memory.add_to_memory(tag, input_text, "LLM")
                        print(
                            "DEBUG: handle_input - memory.add_to_memory (LLM) call completed"
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - Emitting 'Memory addition completed' status (LLM, done=True)"
                        )  # DEBUG
                        await emitter.emit(  # Completion emitter for LLM add
                            description="Memory addition completed.",
                            status="memory_update",
                            done=True,
                        )
                        print(
                            "DEBUG: handle_input - Finished emitting 'Memory addition completed' status (LLM, done=True)"
                        )  # DEBUG
                        print(
                            "DEBUG: handle_input - RETURN: added to memory by LLM's request!"
                        )  # DEBUG
                        return "added to memory by LLM's request!"
                    else:
                        print(
                            "DEBUG: handle_input - Neither user_wants_to_add nor llm_wants_to_add is True"
                        )  # DEBUG
                else:
                    print("DEBUG: handle_input - USE_MEMORY is False")  # DEBUG

                print("DEBUG: handle_input - Reached end of USE_MEMORY block")  # DEBUG
                print(
                    "DEBUG: handle_input - Emitting 'Memory handling logic executed' status (done=True)"
                )  # DEBUG
                await emitter.emit(
                    status="complete",
                    description="Memory handling logic executed.",
                    done=True,
                )  # Default completion emitter
                print(
                    "DEBUG: handle_input - Finished emitting 'Memory handling logic executed' status (done=True)"
                )  # DEBUG
                print(
                    "DEBUG: handle_input - RETURN: Memory handling logic executed."
                )  # DEBUG
                return "Memory handling logic executed."  # Default return

            except Exception as e:  # <---- CATCH ANY EXCEPTION IN THE FUNCTION
                error_message = f"Error in handle_input: {e}"
                print(f"ERROR: handle_input - Exception: {error_message}")  # ERROR LOG
                await emitter.emit(
                    status="error", description=error_message, done=True
                )  # ERROR STATUS EMITTER
                print(
                    "DEBUG: handle_input - Emitting 'Error in handle_input' status (done=True)"
                )  # DEBUG
                return f"Error handling input: {error_message}"  # Return error message

            finally:  # <---- FINALLY BLOCK FOR ABSOLUTE END DEBUG
                print(
                    "DEBUG: handle_input - FINISH - Function execution completed (try/except/finally)"
                )  # FINISH DEBUG

        async def recall_memories(
            self, __event_emitter__: Callable[[dict], Any] = None
        ) -> str:
            """
            Retrieve all stored memories in current file and provide them to the user.

            :return: A structured representation of all memory contents.
            """
            emitter = EventEmitter(__event_emitter__)
            await emitter.emit(
                "Retrieving all stored memories.", status="recall_in_progress"
            )

            all_memories = self.memory.get_all_memories()
            if (
                not all_memories
            ):  # Check if all_memories is an empty dict or contains error key
                message = (
                    "No memory stored."
                    if not isinstance(all_memories, dict)
                    or "error" not in all_memories
                    else all_memories.get("error", message)
                )
                if self.valves.DEBUG:
                    print(message)
                await emitter.emit(
                    description=message,
                    status="recall_complete",
                    done=True,
                )
                return json.dumps({"message": message}, ensure_ascii=False)

            # Correctly format stored memories contents for readability
            formatted_memories = json.dumps(all_memories, ensure_ascii=False, indent=4)

            if self.valves.DEBUG:
                print(f"All stored memories retrieved: {formatted_memories}")

            await emitter.emit(
                description=f"All stored memories retrieved: {formatted_memories}",
                status="recall_complete",
                done=True,
            )

            return f"Memories are : {formatted_memories}"

        async def clear_memories(
            self, user_confirmation: bool, __event_emitter__: Callable[[dict], Any] = None
        ) -> str:
            """
            Clear all stored memories in current file after user confirmation;ask twice the user for confimation.

            :param user_confirmation: Boolean indicating user confirmation to clear memories.
            :return: A message indicating the status of the operation.
            """
            emitter = EventEmitter(__event_emitter__)
            await emitter.emit(
                "Attempting to clear all memory entries.", status="clear_memory_attempt"
            )

            if self.confirmation_pending and user_confirmation:
                clear_result = self.memory.clear_memory()  # Get result from clear_memory
                await emitter.emit(
                    description="All memory entries have been cleared.",
                    status="clear_memory_complete",
                    done=True,
                )
                self.confirmation_pending = False
                return json.dumps(
                    {"message": clear_result}, ensure_ascii=False  # Use result message
                )

            if not self.confirmation_pending:
                self.confirmation_pending = True
                await emitter.emit(
                    description="Please confirm that you want to clear all memories. Call this function again with confirmation.",
                    status="confirmation_required",
                    done=False,
                )
                return json.dumps(
                    {"message": "Please confirm to clear all memories."},
                    ensure_ascii=False,
                )

            await emitter.emit(
                description="Clear memory operation aborted.",
                status="clear_memory_aborted",
                done=True,
            )
            self.confirmation_pending = False
            return json.dumps(
                {"message": "Memory clear operation aborted."}, ensure_ascii=False
            )

        async def refresh_memory(self, __event_emitter__: Callable[[dict], Any] = None):
            """
            Periodically refresh and optimize memory data, includes reindexing.

            :returns: A message indicating the status of the refresh operation.
            """
            emitter = EventEmitter(__event_emitter__)
            await emitter.emit("Starting memory refresh process.")

            if self.valves.DEBUG:
                print("Refreshing memory...")

            if self.valves.USE_MEMORY:
                refresh_message = self.memory.reindex_memory()  # Reindex returns a message

                if self.valves.DEBUG:
                    print(refresh_message)

                await emitter.emit(
                    description=refresh_message, status="memory_refresh", done=True
                )

                return refresh_message

            if self.valves.DEBUG:
                print("Memory refreshed.")

            await emitter.emit(
                status="complete", description="Memory refresh completed.", done=True
            )
            return "Memory refresh completed."  # Added return for completeness

        async def update_memory_entry(
            self,
            index: int,
            tag: str,
            memo: str,
            by: str,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Update an existing memory entry based on its index.

            :param index: The index of the memory entry to update,STARTING FROM 1.
            :param tag: The tag for the memory entry.
            :param memo: The memory information to update.
            :param by: Who is making the update ('user' or 'LLM').
            :returns: A message indicating the success or failure of the update.
            """
            emitter = EventEmitter(__event_emitter__)

            if self.valves.DEBUG:
                print(
                    f"Updating memory index {index} with tag: {tag}, memo: {memo}, by: {by}"
                )

            update_message = self.memory.update_memory_by_index(
                index, tag, memo, by
            )  # Get update message

            await emitter.emit(
                description=update_message, status="memory_update", done=True
            )

            return update_message

        async def add_multiple_memories(
            self,
            memory_entries: list,
            llm_wants_to_add: bool,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Allows the LLM to add multiple memory entries at once.

            :param memory_entries: A list of dictionary entries, each containing tag, memo, by.Usage Examples:
                                       **General Examples:**
                                       `memory_entries = [{"tag": "personal", "memo": "This is a personal note", "by": "LLM"},{"tag": "work", "memo": "Project deadline is tomorrow", "by": "LLM"}]`

                                       **Tag-Specific Examples:**
                                       * **Reminders:**
                                         `memory_entries = [{"tag": "reminder", "memo": "Schedule client follow-up call for Friday", "by": "LLM"}, {"tag": "reminder", "memo": "Remember to review client progress reports", "by": "LLM"}]`

                                       * **Wellness (or Fitness):**
                                         `memory_entries = [{"tag": "wellness", "memo": "Client completed workout successfully, feeling energized", "by": "LLM"}, {"tag": "wellness", "memo": "Client mentioned improved sleep quality this week", "by": "LLM"}]`

                                       * **Mixed Tags:**
                                         `memory_entries = [{"tag": "work", "memo": "New lead from networking event", "by": "LLM"}, {"tag": "reminder", "memo": "Send invoice to client by end of day", "by": "LLM"}, {"tag": "wellness", "memo": "Client achieved personal best on bench press", "by": "LLM"}]`
            :param llm_wants_to_add: Boolean indicating LLM's desire to add the memories.
            :returns: A message indicating the success or failure of the operations.
            """
            emitter = EventEmitter(__event_emitter__)
            responses = []

            if not llm_wants_to_add:
                return "LLM has not requested to add multiple memories."

            for idx, entry in enumerate(memory_entries):
                tag = entry.get("tag", "others")
                memo = entry.get("memo", "")
                by = entry.get("by", "LLM")

                if tag not in self.memory.tag_options:
                    tag = "others"

                if self.valves.DEBUG:
                    print(f"Adding memory {idx+1}: tag={tag}, memo={memo}, by={by}")

                # Add the memory
                add_message = self.memory.add_to_memory(
                    tag, memo, by
                )  # Get add message
                response = f"Memory {idx+1} added with tag {tag} by {by}. Status: {add_message}"  # Include status
                responses.append(response)

                await emitter.emit(
                    description=response, status="memory_update", done=False
                )

            await emitter.emit(
                description="All requested memories have been processed.",
                status="memory_update_complete",
                done=True,
            )

            return "\n".join(responses)

        async def delete_memory_entry(
            self,
            index: int,
            llm_wants_to_delete: bool,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Delete a memory entry based on its index.

            :param index: The index of the memory entry to delete,STARTING FROM 1.
            :param llm_wants_to_delete: Boolean indicating if the LLM has requested the deletion.
            :returns: A message indicating the success or failure of the deletion.
            """
            emitter = EventEmitter(__event_emitter__)

            if not llm_wants_to_delete:
                return "LLM has not requested to delete a memory."

            if self.valves.DEBUG:
                print(f"Attempting to delete memory at index {index}")

            deletion_message = self.memory.delete_memory_by_index(
                index
            )  # Get deletion message

            await emitter.emit(
                description=deletion_message, status="memory_deletion", done=True
            )

            return deletion_message

        async def delete_multiple_memories(
            self,
            indices: list,
            llm_wants_to_delete: bool,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Delete multiple memory entries based on their indices.

            :param indices: A list of indices of the memory entries to delete,STARTING FROM 1.
            :param llm_wants_to_delete: Boolean indicating if the LLM has requested the deletions.
            :returns: A message indicating the success or failure of the deletions.
            """
            emitter = EventEmitter(__event_emitter__)
            responses = []

            if not llm_wants_to_delete:
                return "LLM has not requested to delete multiple memories."

            for index in indices:
                if self.valves.DEBUG:
                    print(f"Attempting to delete memory at index {index}")

                deletion_message = self.memory.delete_memory_by_index(
                    index
                )  # Get deletion message
                responses.append(deletion_message)

                await emitter.emit(
                    description=deletion_message, status="memory_deletion", done=False
                )

            await emitter.emit(
                description="All requested memory deletions have been processed.",
                status="memory_deletion_complete",
                done=True,
            )

            return "\n".join(responses)

        async def create_or_switch_memory_file(
            self, new_file_name: str, __event_emitter__: Callable[[dict], Any] = None
        ) -> str:
            """
            Create a new memory file or switch to an existing one.

            :param new_file_name: The name of the new or existing memory file.
            :returns: A message indicating the success or failure of the operation.
            """
            emitter = EventEmitter(__event_emitter__)

            if self.valves.DEBUG:
                print(f"Switching to or creating memory database file: {new_file_name}")

            switch_message = self.memory.switch_memory_file(
                new_file_name + ".db"
            )  # Switch DB file
            message = f"Memory database file switched to {new_file_name}."

            await emitter.emit(description=message, status="file_switching", done=True)

            return switch_message  # Return the message from memory.switch_memory_file

        async def list_memory_files(
            self, __event_emitter__: Callable[[dict], Any] = None
        ) -> str:
            """
            List available memory files in the designated directory.

            :returns: A message with the list of available memory files.
            """
            emitter = EventEmitter(__event_emitter__)
            memory_files_list = (
                self.memory.list_memory_files()
            )  # Get list of files from memory

            if (
                isinstance(memory_files_list, dict) and "error" in memory_files_list
            ):  # Check for error
                description = memory_files_list["error"]
                status = "file_listing_error"
            else:
                description = "Available memory files: " + ", ".join(
                    memory_files_list
                )
                status = "file_listing_complete"

            if self.valves.DEBUG:
                print(
                    f"Listing memory files in directory: {self.memory.directory}. Files: {description}"
                )

            await emitter.emit(description=description, status=status, done=True)

            return description

        async def current_memory_file(
            self, __event_emitter__: Callable[[dict], Any] = None
        ) -> str:
            """
            Retrieve the name of the currently active memory file.

            :returns: A message indicating the current memory file.
            """
            emitter = EventEmitter(__event_emitter__)

            current_file = self.memory.current_memory_file()  # Get current file from memory

            message = f"Currently using memory file: {current_file}"

            if self.valves.DEBUG:
                print(message)

            await emitter.emit(
                description=message, status="current_file_retrieved", done=True
            )

            return message

        async def delete_memory_file(
            self,
            file_to_delete: str,
            user_confirmation: bool,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Delete a memory file in the designated directory with confirmation and necessary file switching.

            :param file_to_delete: The name of the memory file to delete.
            :param user_confirmation: Boolean indicating user confirmation for deletion.
            :returns: A message indicating the success or failure of the deletion.
            """
            emitter = EventEmitter(__event_emitter__)

            if not user_confirmation:  # Simplified confirmation logic
                self.confirmation_pending = True
                confirmation_message = (
                    "Please confirm that you want to delete the memory database file. "
                    "Call this function again with confirmation=True."
                )
                await emitter.emit(
                    description=confirmation_message,
                    status="confirmation_required",
                    done=False,
                )
                return json.dumps(
                    {
                        "message": "Please confirm to delete the memory database file.",
                        "file": file_to_delete,
                    },
                    ensure_ascii=False,
                )

            if self.confirmation_pending and user_confirmation:
                deletion_message = self.memory.delete_memory_file(
                    file_to_delete
                )  # Delete file via MemoryFunctions
                if (
                    "Cannot delete" in deletion_message
                ):  # Handle case where current file is being deleted
                    await emitter.emit(
                        description=deletion_message, status="deletion_error", done=True
                    )
                    return json.dumps({"message": deletion_message}, ensure_ascii=False)

                self.confirmation_pending = False
                if "deleted successfully" in deletion_message:
                    status = "file_deletion_complete"
                else:
                    status = "deletion_error"  # General deletion error
                await emitter.emit(
                    description=deletion_message, status=status, done=True
                )
                return json.dumps({"message": deletion_message}, ensure_ascii=False)

            await emitter.emit(
                description="Deletion of memory file aborted.",
                status="deletion_aborted",
                done=True,
            )
            self.confirmation_pending = False
            return json.dumps(
                {"message": "Memory file deletion aborted.", "file": file_to_delete},
                ensure_ascii=False,
            )

        async def execute_functions_sequentially(
            self,
            function_calls: list,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> dict:
            """
            Execute a series of functions in sequence.

            :param function_calls: A list of dictionaries each containing 'name' and 'params'.
                                   Example: [{'name': 'handle_input', 'params': {...}}, ...]
            :returns: A dictionary with results of each function call.
            """
            emitter = EventEmitter(__event_emitter__)
            results = {}

            for call in function_calls:
                func_name = call.get("name")
                params = call.get("params", {})

                if hasattr(self, func_name) and callable(getattr(self, func_name)):
                    if self.valves.DEBUG:
                        print(f"Executing function: {func_name} with params: {params}")

                    await emitter.emit(
                        f"Executing {func_name}", status="function_execution", done=False
                    )

                    try:
                        func = getattr(self, func_name)
                        result = await func(__event_emitter__=__event_emitter__, **params)
                        results[func_name] = result

                        await emitter.emit(
                            description=f"{func_name} executed successfully.",
                            status="function_complete",
                            done=False,
                        )
                    except Exception as e:
                        error_msg = f"Error executing {func_name}: {str(e)}"
                        results[func_name] = error_msg

                        await emitter.emit(
                            description=error_msg, status="function_error", done=False
                        )
                else:
                    error_msg = f"Function {func_name} not found or not callable."
                    results[func_name] = error_msg

                    await emitter.emit(
                        description=error_msg, status="function_missing", done=False
                    )

            await emitter.emit(
                description="All requested functions have been processed.",
                status="execution_complete",
                done=True,
            )

            return f"executed successfully :{results}"

        async def download_memory(
            self,
            memory_file_name: str,
            download_all: bool,
            __event_emitter__: Callable[[dict], Any] = None,
        ) -> str:
            """
            Download a specific memory file or all memory files in a tarball;ONLY FOR 14 SECONDS, AND WHEN LLM ANSWERS THE LINK IS EXPIRED

            :param memory_file_name: Name of the memory file or target tarball name.
            :param download_all: Boolean indicating whether to download all memories as a tarball.
            :returns: A message with a link or status of the operation.
            """
            emitter = EventEmitter(__event_emitter__)
            target_file = None  # Initialize target_file outside try block
            httpd = None
            if not download_all and not memory_file_name:  # <--- Add this check
                message = "Error: You must provide a memory file name to download a specific file. To download all files, set 'download_all' to true. Use 'list_memory_files' to see available files."
                await emitter.emit(
                    description=message, status="invalid_input", done=True
                )
                return message
            try:
                if download_all:
                    target_file_path = (
                        self.memory.download_all_memory_files()
                    )  # Get tarball path
                    if (
                        isinstance(target_file_path, dict)
                        and "error" in target_file_path
                    ):  # Check for error
                        message = target_file_path["error"]
                        await emitter.emit(
                            description=message, status="download_error", done=True
                        )
                        return message
                    target_file = target_file_path
                else:
                    target_file_path_or_error = self.memory.download_memory_file(
                        memory_file_name + ".db"
                    )  # Get DB file path
                    if (
                        isinstance(target_file_path_or_error, dict)
                        and "error" in target_file_path_or_error
                    ):  # Check for error
                        message = target_file_path_or_error["error"]
                        await emitter.emit(
                            description=message, status="file_not_found", done=True
                        )
                        return message
                    target_file = target_file_path_or_error

                httpd = None  # Initialize httpd here
                handler = SimpleHTTPRequestHandler
                handler.directory = self.memory.directory
                httpd = TCPServer(("", 0), handler)
                ip, port = httpd.server_address
                server_url = f"http://{ip}:{port}/{os.path.basename(target_file)}"  # Use basename for URL

                # Start the server in a new thread
                server_thread = threading.Thread(
                    target=httpd.serve_forever, daemon=True
                )
                server_thread.start()

                message = f"Download available for 14 seconds in this link: {server_url}"
                await emitter.emit(description=message, status="download", done=True)
                if self.valves.DEBUG:
                    print(message)

                # Give the user time to download
                await asyncio.sleep(14)
                return "TELL THE USER THAT LINK IS EXPIRED AND YOU SHOULD HAVE DOWNLOADED FILES!"

            except Exception as e:
                message = f"Error setting up download server: {str(e)}"
                await emitter.emit(
                    description=message, status="download_error", done=True
                )
                if self.valves.DEBUG:
                    print(message)
                return message  # Return error message in case of exceptions

            finally:
                if httpd:
                    httpd.shutdown()  # Ensure server is shut down
                # No need to delete the file after download in this SQLite version as the DB file persists

            return "Download process completed or encountered an issue."  # General return if not explicitly returned earlier

        def __del__(self):
            """Ensure database connection is closed when the Tools object is deleted."""
            if hasattr(self, "memory") and hasattr(
                self.memory, "close_db_connection"
            ):  # Check if memory and close_db_connection exist
                self.memory.close_db_connection()


class MemoryFunctions:
    def __init__(
        self,
        db_name="chat_memory.db",
        debug=False,
        directory="memory_dbs",  # Renamed directory for clarity
    ):
        self.directory = directory
        os.makedirs(
            self.directory, exist_ok=True
        )  # Ensure the directory exists for DB files
        self.db_name = os.path.join(
            self.directory, db_name
        )  # Path to the database file
        self.debug = debug
        self.tag_options = [
            "personal",
            "work",
            "education",
            "life",
            "person",
            "wellness",
            "relationship",
            "reminder",
            "others",
        ]
        self.conn = self._connect_db()  # Initialize database connection
        self._create_table()  # Ensure table exists

    def _connect_db(self):
        """Establish a database connection."""
        try:
            conn = sqlite3.connect(self.db_name)
            if self.debug:
                print(f"Connected to SQLite database: {self.db_name}")
            return conn
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            return None

    def _create_table(self):
        """Create the memory table if it does not exist."""
        if self.conn is None:
            return

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS memories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag TEXT,
                    memo TEXT,
                    by_who TEXT,
                    last_modified TEXT
                )
                """
            )
            self.conn.commit()
            if self.debug:
                print("Memory table created or already exists.")
            except sqlite3.Error as e:
                print(f"Database table creation error: {e}")

    def switch_memory_file(self, new_db_name: str):  # Renamed to switch_memory_file
        """Switch to a new memory database file in designated directory."""
        if self.conn:
            self.conn.close()  # Close current connection

        self.db_name = os.path.join(self.directory, new_db_name)
        self.conn = self._connect_db()  # Connect to new database
        self._create_table()  # Ensure table exists in new database

        if self.debug:
            print(f"Switched to database file: {self.db_name}")
        return f"Switched to database file: {new_db_name}"  # Return message for function call

    def reindex_memory(self):
        """Reindexing is not directly applicable to SQLite as it's index-based, returning a message."""
        return "Reindexing is not needed for SQLite databases."

    def delete_memory_by_index(self, index: int):
        """Delete memory entry by its index (row ID in SQLite)."""
        if self.conn is None:
            return "No database connection."

        print(
            f"DEBUG: delete_memory_by_index called with index: {index}"
        )  # <---- ADD THIS DEBUG PRINT

        cursor = self.conn.cursor()
        try:
            cursor.execute("DELETE FROM memories WHERE id = ?", (index,))
            if cursor.rowcount > 0:
                self.conn.commit()
                return f"Memory index {index} deleted successfully."
            else:
                return f"Memory index {index} does not exist."
        except sqlite3.Error as e:
            return f"Database error deleting memory index {index}: {e}"

    def update_memory_by_index(self, index: int, tag: str, memo: str, by: str):
        """Update memory entry by its index."""
        if self.conn is None:
            return "No database connection."

        if tag not in self.tag_options:
            tag = "others"

        last_modified = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

        print(
            f"DEBUG: update_memory_by_index called with index: {index}"
        )  # <---- ADD THIS DEBUG PRINT

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE memories
                SET tag = ?, memo = ?, by_who = ?, last_modified = ?
                WHERE id = ?
                """,
                (tag, memo, by, last_modified, index),
            )
            if cursor.rowcount > 0:
                self.conn.commit()
                return f"Memory index {index} updated successfully."
            else:
                return f"Memory index {index} does not exist."
        except sqlite3.Error as e:
            return f"Database error updating memory index {index}: {e}"

    # load_memory and save_memory methods are removed as SQLite handles persistence

    def add_to_memory(self, tag: str, memo: str, by: str):
        """Add a new entry to memory."""
        if self.conn is None:
            return "No database connection."

        if tag not in self.tag_options:
            tag = "others"

        last_modified = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO memories (tag, memo, by_who, last_modified)
                VALUES (?, ?, ?, ?)
                """,
                (tag, memo, by, last_modified),
            )
            self.conn.commit()
            return "Memory added successfully."
        except sqlite3.Error as e:
            return f"Database error adding memory: {e}"

    def retrieve_from_memory(
        self, index: int
    ):  # Changed key to index, assuming index retrieval
        """Retrieve memory by its index (row ID)."""
        if self.conn is None:
            return "No database connection."

        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT tag, memo, by_who, last_modified FROM memories WHERE id = ?",
                (index,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "index": index,  # Added index to the return for clarity
                    "tag": row[0],
                    "memo": row[1],
                    "by": row[2],
                    "last_modified": row[3],
                }
            else:
                return None  # Or return a specific message "Memory index not found"
        except sqlite3.Error as e:
            print(f"Database error retrieving memory by index {index}: {e}")
            return None

    def process_input_for_memory(
        self, input_text: str
    ):  # Unchanged, might need adaptation if needed
        return {"timestamp": str(datetime.datetime.now()), "input": input_text}

    def get_all_memories(self) -> dict:
        """Retrieve all memories from the database."""
        if self.conn is None:
            return {"error": "No database connection."}

        cursor = self.conn.cursor()
        all_memories = {}
        try:
            cursor.execute(
                "SELECT id, tag, memo, by_who, last_modified FROM memories"
            )
            rows = cursor.fetchall()
            for row in rows:
                index, tag, memo, by_who, last_modified = row
                all_memories[index] = {  # Using index as key in dictionary
                    "tag": tag,
                    "memo": memo,
                    "by": by_who,
                    "last_modified": last_modified,
                }
            return all_memories
        except sqlite3.Error as e:
            print(f"Database error retrieving all memories: {e}")
            return {"error": f"Database error: {e}"}

    def clear_memory(self):
        """Clear all memory entries from the database."""
        if self.conn is None:
            return "No database connection."

        cursor = self.conn.cursor()
        try:
            cursor.execute("DELETE FROM memories")
            self.conn.commit()
            return "ALL MEMORIES CLEARED!"
        except sqlite3.Error as e:
            return f"Database error clearing all memories: {e}"

    def list_memory_files(
        self,
    ):  # Renamed to list_memory_files to align with Tools class
        """List available memory database files in the designated directory."""
        memory_files = []
        try:
            for file in os.listdir(self.directory):
                if file.endswith(".db"):  # Looking for .db files now
                    memory_files.append(file)
        except Exception as e:
            print(f"Error listing memory files: {e}")
            return {"error": f"Error accessing directory: {str(e)}"}
        return memory_files

    def current_memory_file(
        self,
    ):  # Renamed to current_memory_file for consistency
        """Return the name of the currently active memory database file."""
        return os.path.basename(self.db_name)

    def delete_memory_file(
        self, file_to_delete: str
    ):  # Renamed to delete_memory_file for consistency
        """Delete a memory database file from the directory."""
        file_path = os.path.join(self.directory, file_to_delete)
        try:
            if (
                os.path.exists(file_path) and file_path != self.db_name
            ):  # Prevent deleting current DB
                os.remove(file_path)
                return f"File '{file_to_delete}' deleted successfully."
            elif file_path == self.db_name:
                return "Cannot delete the currently active database file. Switch to another file first."
            else:
                return f"File '{file_to_delete}' does not exist in the directory."
        except Exception as e:
            return f"Error deleting file '{file_to_delete}': {str(e)}"

    def download_memory_file(
        self, file_to_download: str
    ):  # Renamed and adapted for DB download
        """Prepare a memory database file for download."""
        file_path = os.path.join(self.directory, file_to_download)
        if not os.path.exists(file_path) or not file_path.endswith(".db"):
            return {
                "error": f"Database file '{file_to_download}' not found or invalid."
            }
        return file_path  # Return file path for download handling in Tools class

    def download_all_memory_files(self):  # New function to download all DB files
        """Prepare all memory database files for download as tarball."""
        tarball_path = os.path.join(self.directory, "all_memory_dbs.tar.gz")
        db_files_to_tar = []
        try:
            with tarfile.open(tarball_path, "w:gz") as tar:
                for file in os.listdir(self.directory):
                    if file.endswith(".db"):
                        db_file_path = os.path.join(self.directory, file)
                        tar.add(db_file_path, arcname=file)
                        db_files_to_tar.append(db_file_path)
            return tarball_path  # Return path to the created tarball
        except Exception as e:
            print(f"Error creating tarball of database files: {e}")
            return {"error": f"Error creating tarball: {str(e)}"}

    def close_db_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None  # Reset connection attribute


class EventEmitter:
    def __init__(self, event_emitter: Callable[[dict], Any] = None):
        self.event_emitter = event_emitter

    async def emit(self, description="Unknown state", status="in_progress", done=False):
        if self.event_emitter:
            await self.event_emitter(
                {
                    "type": "status",
                    "data": {
                        "status": status,
                        "description": description,
                        "done": done,
                    },
                }
            )


if __name__ == "__main__":
    # Example of how to run the pipeline (for testing purposes outside OpenWebUI)
    async def main():
        pipeline_instance = Pipeline()
        tools_instance = pipeline_instance.tools

        # Example function call (you'd normally get function calls from the LLM)
        result = await tools_instance.recall_memories()
        print("Recall Memories Result:", result)

        # Example of executing multiple functions
        function_calls_example = [
            {"name": "create_or_switch_memory_file", "params": {"new_file_name": "test_memory_file"}},
            {"name": "add_to_memory", "params": {"tag": "test", "memo": "Test memory entry", "by": "user"}},
            {"name": "recall_memories", "params": {}},
        ]
        results = await tools_instance.execute_functions_sequentially(function_calls_example)
        print("Sequential Function Execution Results:", results)

        # Example of downloading memory files (you might need to adjust paths for your local testing)
        download_result = await tools_instance.download_memory(memory_file_name="test_memory_file", download_all=False)
        print("Download Memory Result:", download_result)


    asyncio.run(main())
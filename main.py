#!/usr/bin/env python3
"""
Main entry point for the RDBMS REPL.
"""

from db.repl import REPL


def main():
    """Start the REPL."""
    repl = REPL()
    repl.run()


if __name__ == "__main__":
    main()

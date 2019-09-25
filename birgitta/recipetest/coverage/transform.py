"""Functions for inserting transform logging into a recipe.
"""

import re

__all__ = ['prepare']


def prepare(content):
    """Prepare the code by inserting assignement checks
    to inspect the counts of each transform.

    Args:
        content (str): The code of the recipe

    Returns:
        A string of the code with line checks inserted.
    """
    lines = content.splitlines()
    assigns = gather_assigns(lines)
    lines_w_checks = insert_assign_checks(lines, assigns)
    return '\n'.join(lines_w_checks)


assign_reg = re.compile("^([a-z][a-zA-Z0-9_]+) = [a-zA-Z]")


def gather_assigns(lines):
    assigns = []
    for i, line in enumerate(lines):
        result = assign_reg.search(line)
        if result:
            assigns.append({
                "var": result.group(1),
                "line_idx": i,
                "line": line
            })
    return assigns


def insert_assign_checks(lines, assigns):
    if len(assigns) == 0:
        return lines
    output_lines = []
    pending_checks = {}
    pending_assigns = {assign["line_idx"]: assign for assign in assigns}

    # Traverse all lines harvesting check points and check fn code
    for line_idx, line in enumerate(lines):
        harvest_checks(pending_checks,
                       line_idx,
                       lines,
                       pending_assigns)

    # construct output file
    for line_idx, line in enumerate(lines):
        insert_checks(output_lines,
                      line_idx,
                      pending_checks)
        output_lines.append(line)

    # Deal with possible insert after last line
    line_after_end_idx = len(lines)
    insert_checks(output_lines,
                  line_after_end_idx,
                  pending_checks)
    # Check that we have no assigns left
    if len(pending_assigns) > 0:
        raise Exception("Unprocessed assign %s" %
                        (repr(pending_assigns)))
    return output_lines


def harvest_checks(pending_checks, line_idx, lines, pending_assigns):
    assign_line_indices = list(pending_assigns.keys())
    for assign_line_idx in assign_line_indices:
        assign = pending_assigns.pop(line_idx, None)
        if assign:
            insert_point = suitable_insert_point(lines, line_idx+1)
            assign_check_line = assign_check(assign)
            if insert_point not in pending_checks:
                pending_checks[insert_point] = []
            pending_checks[insert_point].append(assign_check_line)


def insert_checks(output_lines,
                  line_idx,
                  pending_checks):
    check_lines = pending_checks.pop(line_idx, None)
    if check_lines:
        for check_line in check_lines:
            output_lines.append(check_line)


def assign_check(assign):
    if not assign:
        return None
    return "log_transform(%s, %d, \"%s\")" % (
        assign["var"],
        assign["line_idx"] + 1,
        escape_line(assign["line"])
    )


def escape_line(line):
    ret = line.replace("'", "")
    ret = ret.replace('"', '')
    return ret.replace('\\', '')


def suitable_insert_point(lines, orig_line_idx):
    idx = orig_line_idx
    max_idx = len(lines) - 1
    while idx <= max_idx:
        line = lines[idx]
        if multiline_stmt(line):
            idx = idx + 1
            continue
        return idx
    return max_idx + 1  # after last line


# A statement with chars without preceding white space
root_stmt_reg = re.compile("^([a-z][a-zA-Z0-9_]+)")
comment_reg = re.compile("^(#)")


def multiline_stmt(line):
    # Empty line is NOT considered outside a multiline stmt
    if line == "":
        return False
    # Root stmts are not multiline stmt
    if root_stmt_reg.search(line):
        return False
    # Root comments stmts are not multiline stmt
    if comment_reg.search(line):
        return False
    return True

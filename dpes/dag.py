"""Dependency graph checks. Pure, and unaware of the job format (plan/08)."""

_NEW, _ON_PATH, _DONE = 0, 1, 2


def find_cycle(needs):
    """A cycle in `needs` (name -> names it depends on), as a path, or None.

    Iterative depth-first search in name order, so a job of any size cannot
    exhaust the stack and the same graph always reports the same cycle.
    Names that are not keys of `needs` are ignored.
    """
    state = {}
    for root in sorted(needs):
        if state.get(root, _NEW) != _NEW:
            continue
        path = [root]
        state[root] = _ON_PATH
        pending = [iter(sorted(needs[root]))]
        while pending:
            for name in pending[-1]:
                if name not in needs:
                    continue
                seen = state.get(name, _NEW)
                if seen == _ON_PATH:
                    return path[path.index(name):] + [name]
                if seen == _NEW:
                    state[name] = _ON_PATH
                    path.append(name)
                    pending.append(iter(sorted(needs[name])))
                    break
            else:  # every dependency of path[-1] explored without a cycle
                state[path.pop()] = _DONE
                pending.pop()
    return None

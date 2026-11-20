import copy
from typing import Annotated, get_args, get_origin, get_type_hints

from apixis.core.graph.base import AutoMerge, KeepRef


def copy_state(
    state: dict,
    keep_ref_keys: frozenset[str],
) -> dict:
    """Copy graph state while preserving fields marked with ``KeepRef``.

    Args:
        state: State mapping to copy.
        keep_ref_keys: Fields whose values must retain object identity.

    Raises:
        TypeError: If ``state`` is not a dictionary.
    """
    if not isinstance(state, dict):
        raise TypeError("Graph state must be a dict.")

    if not keep_ref_keys:
        return copy.deepcopy(state)

    keep_refs = {key: state[key] for key in keep_ref_keys if key in state}

    # Exclude kept fields before deepcopy so resource-like values do not need
    # to support copying. Rebuilding in original key order also preserves the
    # alias behavior of ordinary fields.
    copied_values = copy.deepcopy(
        {key: value for key, value in state.items() if key not in keep_refs}
    )
    return {
        key: (keep_refs[key] if key in keep_refs else copied_values[key])
        for key in state
    }


def parse_state_schema(
    state_schema: type | None,
) -> tuple[frozenset[str], frozenset[str]]:
    """Resolve annotations once for the compiled graph's two state policies.

    TypedDict and regular annotated classes are accepted. Both marker classes
    and instances are supported. None disables both policies. Invalid classes
    and unresolved forward references fail during graph compilation.
    """
    if state_schema is None:
        return frozenset(), frozenset()
    if not isinstance(state_schema, type):
        raise TypeError(
            "`state_schema` must be a class or None, "
            f"got {type(state_schema).__name__}."
        )
    merge: set[str] = set()
    keep: set[str] = set()
    for key, annotation in get_type_hints(state_schema, include_extras=True).items():
        if get_origin(annotation) is Annotated:
            for marker in get_args(annotation)[1:]:
                if marker is AutoMerge or isinstance(marker, AutoMerge):
                    merge.add(key)
                if marker is KeepRef or isinstance(marker, KeepRef):
                    keep.add(key)
    return frozenset(merge), frozenset(keep)
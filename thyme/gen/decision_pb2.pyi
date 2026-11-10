from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DecisionSpec(_message.Message):
    __slots__ = ("provider", "model", "allow_alias", "state", "question", "deadline_ms", "retries")
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    ALLOW_ALIAS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    RETRIES_FIELD_NUMBER: _ClassVar[int]
    provider: str
    model: str
    allow_alias: bool
    state: _containers.RepeatedScalarFieldContainer[str]
    question: Question
    deadline_ms: int
    retries: int
    def __init__(self, provider: _Optional[str] = ..., model: _Optional[str] = ..., allow_alias: bool = ..., state: _Optional[_Iterable[str]] = ..., question: _Optional[_Union[Question, _Mapping]] = ..., deadline_ms: _Optional[int] = ..., retries: _Optional[int] = ...) -> None: ...

class Question(_message.Message):
    __slots__ = ("choice", "score", "noul")
    class ChoiceOption(_message.Message):
        __slots__ = ("key", "description")
        KEY_FIELD_NUMBER: _ClassVar[int]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        key: str
        description: str
        def __init__(self, key: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...
    class Choice(_message.Message):
        __slots__ = ("instructions", "options")
        INSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
        OPTIONS_FIELD_NUMBER: _ClassVar[int]
        instructions: str
        options: _containers.RepeatedCompositeFieldContainer[Question.ChoiceOption]
        def __init__(self, instructions: _Optional[str] = ..., options: _Optional[_Iterable[_Union[Question.ChoiceOption, _Mapping]]] = ...) -> None: ...
    class Score(_message.Message):
        __slots__ = ("instructions", "levels")
        INSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
        LEVELS_FIELD_NUMBER: _ClassVar[int]
        instructions: str
        levels: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, instructions: _Optional[str] = ..., levels: _Optional[_Iterable[str]] = ...) -> None: ...
    class Noul(_message.Message):
        __slots__ = ("instructions",)
        INSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
        instructions: str
        def __init__(self, instructions: _Optional[str] = ...) -> None: ...
    CHOICE_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    NOUL_FIELD_NUMBER: _ClassVar[int]
    choice: Question.Choice
    score: Question.Score
    noul: Question.Noul
    def __init__(self, choice: _Optional[_Union[Question.Choice, _Mapping]] = ..., score: _Optional[_Union[Question.Score, _Mapping]] = ..., noul: _Optional[_Union[Question.Noul, _Mapping]] = ...) -> None: ...

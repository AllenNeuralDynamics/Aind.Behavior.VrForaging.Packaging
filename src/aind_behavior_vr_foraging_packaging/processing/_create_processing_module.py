import logging
import typing as ty

from ndx_events import NdxEventsNWBFile
from pynwb.base import ProcessingModule

from .._base import AbstractProcessor

logger = logging.getLogger(__name__)


class CreateProcessingModuleProcessor(AbstractProcessor):
    _MODULE_NAME: ty.ClassVar[str] = "behavior"
    _DESCRIPTION: ty.ClassVar[str] = "Processing module for behavior data"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        _nwb = ty.cast(ty.Any, nwb_file)
        if self._MODULE_NAME in _nwb.processing:
            logger.warning(
                "Processing module '%s' already exists in NWB file. Skipping.",
                self._MODULE_NAME,
            )
            return nwb_file
        processing_module = ProcessingModule(name=self._MODULE_NAME, description=self._DESCRIPTION)
        _nwb.add_processing_module(processing_module)
        return nwb_file

    @classmethod
    def module_name(cls) -> str:
        return cls._MODULE_NAME

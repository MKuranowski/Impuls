from pathlib import Path
from typing import ClassVar, Mapping, Optional
from unittest import TestCase

from impuls import DBConnection, LocalResource, PipelineOptions, TaskRuntime
from impuls.resource import ManagedResource
from impuls.tools.testing_mocks import MockFile, MockResource

FIXTURES_DIR = Path(__file__).with_name("fixtures")


class AbstractTestTask:
    # NOTE: Nested classes are necessary to prevent abstract test cases
    #       from being discovered and run.
    #       See https://stackoverflow.com/a/50176291.

    class Template(TestCase):
        db_name: ClassVar[Optional[str]] = "wkd.db"
        resources: ClassVar[Mapping[str, MockResource | LocalResource]] = {}
        options: ClassVar[PipelineOptions] = PipelineOptions()

        workspace: MockFile
        runtime: TaskRuntime

        def _prepare_db(self) -> DBConnection:
            db_path = self.workspace.path / "impuls.db"
            if self.db_name:
                return DBConnection.cloned(from_=Path(FIXTURES_DIR, self.db_name), in_=db_path)
            else:
                return DBConnection.create_with_schema(db_path)

        def _prepare_resource(
            self,
            name: str,
            resource: MockResource | LocalResource,
        ) -> ManagedResource:
            match resource:
                case LocalResource():
                    resource.update_last_modified(fake_fetch_time=True)
                    return ManagedResource(
                        resource.path, resource.last_modified, resource.fetch_time
                    )

                case MockResource():
                    path = self.workspace.path / name
                    path.write_bytes(resource.content)
                    return ManagedResource(path, resource.last_modified, resource.fetch_time)

            raise RuntimeError(f"Disallowed Resource type in fixture: {type(resource).__name__}")

        def _prepare_resources(self) -> dict[str, ManagedResource]:
            return {
                name: self._prepare_resource(name, resource)
                for name, resource in self.resources.items()
            }

        def setUp(self) -> None:
            super().setUp()
            self.workspace = MockFile(directory=True)
            self.runtime = TaskRuntime(
                db=self._prepare_db(),
                resources=self._prepare_resources(),
                options=self.options,
            )

        def tearDown(self) -> None:
            super().tearDown()
            self.runtime.db.close()
            self.workspace.cleanup()

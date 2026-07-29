add_library(
  ducklake_metadata_manager OBJECT
  postgres_metadata_manager.cpp postgres_metadata_optimizations.cpp
  quack_metadata_manager.cpp sqlite_metadata_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ducklake_metadata_manager>
    PARENT_SCOPE)

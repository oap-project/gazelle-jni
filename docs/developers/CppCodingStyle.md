---
layout: page
title: CPP Code Style
nav_order: 4
parent: Developer Overview
---
# Gluten CPP Core Guidelines

This is a set of CPP core guidelines for Gluten. The aim is to make the codebase
simpler, more efficient, more maintainable by promoting consistency and according
to best practices.

## Philosophy

Philosophical rules are generally not measurable. However, they are valuable. For
Gluten CPP coding, there are a few Philosophical rules as the following.

* Write in ISO Standard C++.
* Standard API first, the CPP programming APIs are priority to system calls.
* Write code consistently. It's good for understanding and maintaining.
* Keep simple, making code clear and easy to read.
* Optimize code for reader, not for writer. Thus, more time will be spent reading
	code than writing it.
* Make it work, and then make it better or faster.
* Don't import any complexity if possible. Collaborate with minimal knowledge
	consensus.

## Code Formatting

Many aspects of C++ coding style will be covered by clang-format, such as spacing,
line width, indentation and ordering (for includes, using directives and etc).

* Always ensure your code is compatible with clang-format-15 for Velox backend.
* `dev/formatcppcode.sh` is provided for formatting Velox CPP code.

To format cmake files like CMakeLists.txt & *.cmake, `cmake-format` is required to
be installed. Here is an example.

```
apt install python3-pip -y
pip3 install --user cmake-format
cmake-format --first-comment-is-literal True --in-place cpp/velox/CMakeLists.txt
```

## Naming Conventions

* Use **PascalCase** for types (class, struct, enum, type alias, type
  template parameter) and file name.
* Use **camelCase** for function, member and local variable, and non-type
  template parameter.
* Use **camelCase_** for private and protected member variable.
* Use **snake_case** for namespace name and build target.
* Use **UPPER_SNAKE_CASE** for macro.
* Use **kPascalCase** for static constant and enumerator.

## Designs

* No over design.
* No negation of negation, `isValid` is better than `isNotInvalid`.
* Avoid corner case, and common case first.
* Express ideas directly, don't let me think.
* Make a check for the arguments in the interface between modules, and don't make a check
	in the inner implementation, use `assert` in the private implementation instead of too
	much safe check.

## Source File & Header File

* All header files must have a single-inclusion guard using `#pragma once`
* Always use `.h` as header file suffix, not `.hpp`.
* Always use `.cc` as source file suffix, neither `.cpp` nor `.cxx`.
* One file should contain one main class, and the file name should be consistent with
	the main class name.
	* Obvious exception: files used for defining various misc functions.
* If a header file has a corresponding source file, they should have the same file
	name with different suffix, such as `a.h vs a.cc`.
* If a function is declared in the file `a.h`, ensure it's defined in the corresponding
	source file `a.cc`, do not define it in other files.
* No deep source directory for CPP files, not do it as JAVA.
* Include header files should satisfy the following rules.
	* Include the necessary header files, which means the source file (.cc) containing 
	the only one line `#include "test.h"` can be compiled successfully without
	including any other header files.
	* Do not include any unnecessary header files, the more including, the slower
	compiling.
	* In one word, no more, no less, just as needed.

## Class

* Base class name doesn't end with Base, use `Backend` instead of `BackendBase`.
* Ensure one class does one thing, and follows the single responsibility principle.
* No big class, No huge class, No too much interfaces.
* Distinguish interface from implementation, make implementations private.
* When designing a class hierarchy, distinguish between interface inheritance
	and implementation inheritance.
	* Ensure that public inheritance represent the relation of `is-a`.
	* Ensure that private inheritance represent the relation of `implements-with`.
* Don't make a function `virtual` without reason.
* Ensure the polymorphic base class has a `virtual` destructor.
* Use `override` to make overriding explicit and to make the compiler work.
* Use `const` to mark the member function read-only as far as possible.
* When you try to define a `copy constructor` or a `operator=` for a class, remember
	the `Rule of three/five/zero`.

## Function

* Make functions short and simple.
* Calling a meaningful function is more readable than writing too many statements
	in place, but the performance-sensitive code path is an exception.
* Give the function a good name, how to check whether the function name is good
	or not.
	* When you read it loudly, you feel smooth.
	* The information can be represented by arguments should not be encoded into 
		the function name. such as. use `get(size_t index)` instead of `getByIndex`.
* A function should focus on a single logic operation.
* A function should do as the name meaning.
	* do everything covered by the function name
	* don't do anything not covered by the function name

## Variable

* Make variable names simple and meaningful.
* Don't group all your variables at the top of the scope, it's an outdated habit.
* Declare variables as close to the usage point as possible.

## Constant

* Prefer const variables to using preprocessor (`#define`) to define constant values.

## Macro

* Macros downgrade readability, break mind, and affect debug.
* Macros have side effects.
* Use macros cautiously and carefully.
* Consider using `const` variables or `inline` functions to replace macros.
* Consider defining macros with the wrap of `do {...} while (0)`
* Avoid using 3rd party library macros directly.

## Namespace

* Don't `using namespace xxx` in header files. Instead, you can do this in source files.
  But it's still not encouraged.
* Place all Gluten CPP codes under `namespace gluten` because one level namespace
	is enough. No nested namespace. Nested namespaces bring mess.
* The anonymous namespace is recommended for defining file level classes, functions
	and variables. It's used to place file scoped static functions and variables.

## Resource Management

* Use handles and RAII to manage resources automatically.
* Immediately give the result of an explicit resource allocation to a manager 
	object.
* Prefer scoped objects and stack objects.
* Use raw pointers to denote individual objects.
* Use `pointer + size_t` to denote array objects if you don't want to use containers.
* A raw pointer (a `T*`) is non-owning.
* A raw reference (a `T&`) is non-owning.
* Understand the difference of `unique_ptr`, `shared_ptr`, `weak_ptr`.
	* `unique_ptr` represents ownership, but not share ownership. `unique_ptr` is
		equivalent to RAII, release the resource when the object is destructed.
	* `shared_ptr` represents shared ownership by use-count. It is more expensive 
		that `unique_ptr`.
	* `weak_ptr` models temporary ownership. It is useful in breaking reference cycles 
		formed by objects managed by `shared_ptr`.
* Use `unique_ptr` or `shared_ptr` to represent ownership.
* Prefer `unique_ptr` over `shared_ptr` unless you need to share ownership.
* Use `make_unique` to make `unique_ptr`s.
* Use `make_shared` to make `shared_ptr`s.
* Take smart pointers as parameters only to explicitly express lifetime semantics.
* **For general use**, take `T*` or `T&` arguments rather than smart pointers.

## Exception

* The exception specifications are changing always. The difference between various CPP
	standards is big, so we should use exception cautiously in Gluten.
* Prefer `return code` to throwing exceptions.
* Prefer compile-time checking to run-time checking.
* Encapsulate messy constructors, rather than spreading through the code.

## Code Comment

* Add necessary comments. The comment is not the more the better, also not the
	less the better.
* Good comment makes obscure code easily understood. It's unnecessary to add comments for
    quite obvious code.

## References

* [CppCoreGuidelines](https://github.com/fluz/CppCoreGuidelines)
* [Velox CODING_STYLE](https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md)
* Thanks Gluten developers for their wise suggestions and helps.

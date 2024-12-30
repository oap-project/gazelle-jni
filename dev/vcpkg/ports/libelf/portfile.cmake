vcpkg_download_distfile(ARCHIVE
    URLS "https://fossies.org/linux/misc/old/libelf-0.8.13.tar.gz"
    FILENAME "libelf-0.8.13.tar.gz"
    SHA512 d2a4ea8ccc0bbfecac38fa20fbd96aefa8e86f8af38691fb6991cd9c5a03f587475ecc2365fc89a4954c11a679d93460ee9a5890693112f6133719af3e6582fe
    OPTIONS "--insecure"
)

vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES install.patch
)

vcpkg_configure_make(SOURCE_PATH ${SOURCE_PATH} AUTOCONFIG)
vcpkg_install_make()
vcpkg_fixup_pkgconfig()

# file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
# file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share")
# file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING.LIB")
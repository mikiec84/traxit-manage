import os
import re


def docker_deps(basepath):
    """Get docker image dependencies.

    Args:
        basepath (str): Path to the package

    Returns:
        set: Dependencies names.
    """
    docker_file = os.path.join(basepath, 'Dockerfile')
    if os.path.exists(docker_file):
        with open(docker_file, 'r') as f:
            dockerfile = f.read()
        re_dockerfile = re.compile('FROM (.+)')
        docker_from = re_dockerfile.findall(dockerfile)
        assert len(docker_from) <= 1
        if docker_from:
            docker_from = docker_from[0]
            docker_image_name_tag = docker_from.rsplit('/', 1)[-1]
            docker_image_name = docker_image_name_tag.split(':', 1)[0]
            return {docker_image_name}
        else:
            return set()
    else:
        return set()


def python_deps(basepath):
    """Get python dependencies from requirements.txt and test_requirements.txt.

    Args:
        basepath (str): Path to the package

    Returns:
        set: Requirements names.
    """
    requirements = []

    requirements_file = os.path.join(basepath, 'requirements.txt')
    test_requirements_file = os.path.join(basepath, 'test_requirements.txt')
    app_requirements_file = os.path.join(basepath, 'app', 'requirements.txt')
    app_test_requirements_file = os.path.join(basepath, 'app', 'test_requirements.txt')

    for req_file in (requirements_file, test_requirements_file, app_requirements_file, app_test_requirements_file):
        if os.path.exists(req_file):
            with open(req_file, 'r') as f:
                requirements.extend(f.readlines())

    requirements_names = set(filter(None, map(lambda x: re.split('==|>=|>|<=|<', x)[0], requirements)))
    return requirements_names


def deps(basepath):
    """Gather all dependencies for this package

    Args:
        basepath (str): Path to the package

    Returns:
        set: Dependencies names.
    """
    return python_deps(basepath) | docker_deps(basepath)

if __name__ == '__main__':
    basepath = os.path.dirname(__file__)
    dependencies = deps(basepath)
    for dep in dependencies:
        print dep

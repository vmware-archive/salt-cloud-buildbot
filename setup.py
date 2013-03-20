#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4 ts=4 fenc=utf-8 ft=python cc=100 tw=99 et
'''
    setup.py
    ~~~~~~~~

    Salt Cloud Buildbot Latent Slave support for Buildbot

    :codeauthor: :email:`Pedro Algarvio (pedro@algarvio.me)`
    :copyright: © 2013 by the SaltStack Team, see AUTHORS for more details.
    :license: Apache 2.0, see LICENSE for more details.
'''

import os
from setuptools import setup

import saltcloud_buildbot as package


REQUIREMENTS = ['Distribute']
REQUIREMENTS_FILE = os.path.join(os.path.dirname(__file__), 'requirements.txt')
if os.path.isfile(REQUIREMENTS_FILE):
    with open(REQUIREMENTS_FILE) as orf:
        REQUIREMENTS.extend(
            [line for line in orf.read().split('\n') if line]
        )


setup(name=package.__package_name__,
      version=package.__version__,
      author=package.__author__,
      author_email=package.__email__,
      url=package.__url__,
      description=package.__summary__,
      long_description=package.__description__,
      license=package.__license__,
      platforms='Linux',
      keywords='Salt Cloud Latent Slave for Buildbot',
      packages=['saltcloud_buildbot'],
      install_requires=REQUIREMENTS,
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Web Environment',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: Apache 2.0 License',
          'Operating System :: Linux',
          'Programming Language :: Python',
          'Topic :: Utilities',
      ]
)

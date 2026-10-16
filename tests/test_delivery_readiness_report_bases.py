"""P03/V09-V14: source equivalence is exact and independent of timestamps."""

import pytest

from okto_pulse.core.domain.verification_report import parse_verification_report, verification_report_source_issue
from test_verification_report import payload


@pytest.mark.parametrize('method', ['inspection', 'static_analysis', 'demonstration'])
def test_V09_V10_V11_wrong_source_or_revision_cannot_borrow_a_later_timestamp(method):
    value = payload(method)
    value['observed_at'] = '2099-01-01T00:00:00Z'
    value['sources'] = [dict(reference='repo:delivered', revision='a' * 40, sha256='a' * 64)]
    report = parse_verification_report(value)
    for expected in ((('repo:other', 'a' * 40),), (('repo:delivered', 'b' * 40),)):
        assert verification_report_source_issue(report, expected) == 'verification_base_mismatch'


def test_V12_V14_every_delivered_source_is_required_but_unrelated_dependencies_are_not_delivery():
    value = payload()
    value['sources'] = [dict(reference='repo:a', revision='a' * 40, sha256='a' * 64),
                        dict(reference='repo:dependency', revision='d' * 40, sha256='d' * 64)]
    expected = (('repo:a', 'a' * 40), ('repo:b', 'b' * 40))
    assert verification_report_source_issue(parse_verification_report(value), expected) == 'verification_base_mismatch'
    value['sources'].append(dict(reference='repo:b', revision='b' * 40, sha256='b' * 64))
    assert verification_report_source_issue(parse_verification_report(value), expected) is None


@pytest.mark.parametrize('expected', [(), (('', 'a' * 40),), (('repo:a', ''),)])
def test_V13_unresolved_canonical_source_is_never_success(expected):
    assert verification_report_source_issue(parse_verification_report(payload()), expected) == 'verification_base_unresolved'

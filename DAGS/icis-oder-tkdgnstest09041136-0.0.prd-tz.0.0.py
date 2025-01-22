from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='tkdgnstest09041136',WORKFLOW_ID='6e0d1d5a8c5e4fb59113c4a7502603b9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-tkdgnstest09041136-0.0.prd-tz.0.0'
    ,'schedule_interval':'20 13 * * *'
    ,'start_date': datetime(2024, 9, 4, 13, 18, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6e0d1d5a8c5e4fb59113c4a7502603b9')

    test_vol = []
    test_volMnt = []
    test_vol.append(getVolume('shared-volume','shared-volume'))
    test_volMnt.append(getVolumeMount('shared-volume','5677'))

    test_vol.append(getVolume('tkdgns','tkdgns'))
    test_volMnt.append(getVolumeMount('tkdgns','1234'))

    test_env = [getICISConfigMap('icis-oder-availabilitytest-configmap'), getICISConfigMap('icis-oder-availabilitytest-configmap2'), getICISSecret('icis-oder-availabilitytest-secret')]
    test_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    test_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    test = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6960a476a66948abae49969e52aa11e8',
        'volumes': test_vol,
        'volume_mounts': test_volMnt,
        'env_from':test_env,
        'task_id':'test',
        'image':'/icis/icis-oder-availabilitytest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6e0d1d5a8c5e4fb59113c4a7502603b9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        test,
        Complete
    ]) 

    # authCheck >> test >> Complete
    workflow









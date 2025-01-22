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
                , WORKFLOW_NAME='WC_CBOL210B',WORKFLOW_ID='e74232d772ad4e1ca7501c4647fbf090', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOL210B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e74232d772ad4e1ca7501c4647fbf090')

    cbol210bJob_vol = []
    cbol210bJob_volMnt = []
    cbol210bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbol210bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbol210bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbol210bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol210bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol210bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '93f3296abcf9456eafae1d154875a1aa',
        'volumes': cbol210bJob_vol,
        'volume_mounts': cbol210bJob_volMnt,
        'env_from':cbol210bJob_env,
        'task_id':'cbol210bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.22',
        'arguments':["--job.name=cbol210bJob", "pgmNm=cbol210b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e74232d772ad4e1ca7501c4647fbf090')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol210bJob,
        Complete
    ]) 

    # authCheck >> cbol210bJob >> Complete
    workflow









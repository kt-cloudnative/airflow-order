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
                , WORKFLOW_NAME='ET_CBOL333B',WORKFLOW_ID='79b6426804ca40ffb1d879e4a009dd42', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOL333B-0.4.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('79b6426804ca40ffb1d879e4a009dd42')

    cbol333bJob_vol = []
    cbol333bJob_volMnt = []
    cbol333bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbol333bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbol333bJob_env = [getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISConfigMap('icis-oder-etcterr-batch-configmap2'), getICISSecret('icis-oder-etcterr-batch-secret')]
    cbol333bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol333bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol333bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4f9a829a982c4f248df66e4e227b2ee5',
        'volumes': cbol333bJob_vol,
        'volume_mounts': cbol333bJob_volMnt,
        'env_from':cbol333bJob_env,
        'task_id':'cbol333bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.1',
        'arguments':["--job.name=cbol333bJob", "endTranDate=20240731", "ofcCd=710571", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('79b6426804ca40ffb1d879e4a009dd42')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol333bJob,
        Complete
    ]) 

    # authCheck >> cbol333bJob >> Complete
    workflow








